import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { JobHooks, JobId, JobKind, JobOutcome } from '@deepseek-ai/dsh-jobs'
import { CallId } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import * as ToolJobs from '@deepseek-ai/dsh-tool-jobs'
import ToolRuntime from '@deepseek-ai/dsh-tools'
import MonitorableJobRegistry from '../src/registry.ts'
import * as MonitorTool from '../src/tool.ts'
import { BoundedTextQueue, LineFramer, retainUtf8Tail } from '../src/shared.ts'

describe('stream utilities', () => {
  it('frames Bash LF output across chunks', () => {
    const framer = new LineFramer()
    expect(framer.push('first\nsec')).toEqual(['first'])
    expect(framer.push('ond\n')).toEqual(['second'])
    expect(framer.flush()).toEqual([])
  })

  it('frames PowerShell CRLF and flushes a final partial line', () => {
    const framer = new LineFramer()
    expect(framer.push('error one\r\nerror')).toEqual(['error one'])
    expect(framer.flush()).toEqual(['error'])
  })

  it('retains a bounded UTF-8-safe output tail with a loss marker', () => {
    expect(retainUtf8Tail('abc错误', 6)).toBe('错误')
    const queue = new BoundedTextQueue(6)
    queue.push('abc错误')
    expect(queue.take()).toContain('错误')
    expect(queue.take()).toBe('')
  })
})

class StreamProducer {
  private chunks: string[] = []
  private resolveDone!: (outcome: JobOutcome) => void
  readonly done = new Promise<JobOutcome>((resolve) => { this.resolveDone = resolve })
  readCalls = 0
  cancelCalls = 0
  throwOnRead = false

  emit(text: string): void { this.chunks.push(text) }

  finish(outcome: JobOutcome = { status: 'completed', detail: 'exit code: 0' }): void {
    this.resolveDone(outcome)
  }

  hooks(): JobHooks {
    return {
      cancel: () => {
        this.cancelCalls += 1
        this.resolveDone({ status: 'killed', detail: 'cancelled' })
      },
      done: this.done,
      readOutput: () => {
        this.readCalls += 1
        if (this.throwOnRead) throw new Error('stream unavailable')
        return this.chunks.shift() ?? ''
      },
    }
  }
}

async function harness() {
  const ctx = new Context()
  await ctx.plugin(SystemPrompt)
  await ctx.plugin(ToolRuntime)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(MonitorableJobRegistry)
  await ctx.plugin(ToolJobs)
  await ctx.plugin(MonitorTool, { batchWindowMs: 25 })

  const followup = vi.fn()
  const inject = vi.fn()
  const scope = ctx.plugin(() => {})
  const id = SessionId(`monitor-owner-${Math.random()}`)
  const agent = {
    id,
    ctx: scope.ctx,
    status: 'idle',
    followup,
    inject,
    session: { id, header: { version: 0, id, createdAt: 0, cwd: process.cwd() } },
  } as unknown as Agent
  ctx.agents.register(agent)
  return { ctx, agent, followup, inject }
}

function startStream(
  ctx: Context,
  owner: Agent,
  producer: StreamProducer,
  kind: 'bash' | 'pwsh' = 'bash',
): JobId {
  return ctx.jobs.start({
    kind: kind as JobKind,
    label: `${kind} backend service`,
    owner,
    outputLimitBytes: 64_000,
    run: () => producer.hooks(),
  })
}

async function startMonitor(ctx: Context, agent: Agent, target: JobId, pattern = 'error|fatal') {
  const result = await ctx.tools.execute({
    signal: new AbortController().signal,
    callId: CallId(`monitor-${target}`),
    name: 'job_monitor',
    arguments: {
      job_id: target,
      description: 'backend errors',
      pattern,
    },
    agent,
  })
  if (result.isError) throw new Error(result.error.message)
  return (result.value as { jobId: string }).jobId as JobId
}

describe.each([
  ['bash', '\n'],
  ['pwsh', '\r\n'],
] as const)('%s background job integration', (kind, newline) => {
  it('tees one process output to job_output and Monitor without replaying old output', async () => {
    const { ctx, agent, followup } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer, kind)
    producer.emit(`before subscription${newline}`)

    const monitor = await startMonitor(ctx, agent, target)
    expect(ctx.jobs.read(target, agent).text).toContain('before subscription')
    expect(followup).not.toHaveBeenCalled()

    producer.emit(`routine health check${newline}backend ERROR: database unavailable${newline}`)
    await vi.waitFor(() => expect(followup).toHaveBeenCalledOnce(), { timeout: 1_500 })

    const targetOutput = ctx.jobs.read(target, agent).text
    expect(targetOutput).toContain('routine health check')
    expect(targetOutput).toContain('backend ERROR: database unavailable')
    const monitorOutput = ctx.jobs.read(monitor, agent).text
    expect(monitorOutput).not.toContain('routine health check')
    expect(monitorOutput).toContain('backend ERROR: database unavailable')
    expect(ctx.jobs.get(target, agent).status).toBe('running')

    producer.finish()
    expect((await ctx.jobs.wait(monitor, 1_000, agent)).status).toBe('completed')
  })
})

describe('Registry compatibility and lifecycle', () => {
  it('does not poll an unmonitored stream and preserves the original consuming read', async () => {
    const { ctx, agent } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer)
    producer.emit('ordinary output\n')
    await new Promise(resolve => setTimeout(resolve, 150))
    expect(producer.readCalls).toBe(0)
    expect(ctx.jobs.read(target, agent).text).toBe('ordinary output\n')
    expect(producer.readCalls).toBe(1)
    producer.finish()
  })

  it('stopping a Monitor leaves its target job running', async () => {
    const { ctx, agent } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer)
    const monitor = await startMonitor(ctx, agent, target)

    const listed = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: CallId('list-monitor-jobs'),
      name: 'job_list',
      arguments: {},
      agent,
    })
    expect(listed.isError).toBe(false)
    expect(JSON.stringify(listed.isError ? null : listed.value)).toContain(target)
    expect(JSON.stringify(listed.isError ? null : listed.value)).toContain(monitor)

    const killed = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: CallId('kill-monitor-job'),
      name: 'job_kill',
      arguments: { job_id: monitor, reason: 'no longer needed' },
      agent,
    })
    expect(killed.isError).toBe(false)
    expect(JSON.stringify(killed.isError ? null : killed.value)).toContain('cancellation-requested')
    expect((await ctx.jobs.wait(monitor, 1_000, agent)).status).toBe('killed')
    expect(ctx.jobs.get(target, agent).status).toBe('running')
    expect(producer.cancelCalls).toBe(0)
    producer.finish()
  })

  it('flushes a final partial line when the target exits', async () => {
    const { ctx, agent, followup } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer)
    const monitor = await startMonitor(ctx, agent, target, 'fatal')
    producer.emit('FATAL final line without newline')
    producer.finish()

    expect((await ctx.jobs.wait(monitor, 1_000, agent)).status).toBe('completed')
    expect(ctx.jobs.read(monitor, agent).text).toContain('FATAL final line without newline')
    expect(JSON.stringify(followup.mock.calls)).toContain('FATAL final line without newline')
  })

  it('contains an output-read failure without killing the target process', async () => {
    const { ctx, agent } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer)
    const monitor = await startMonitor(ctx, agent, target)
    producer.throwOnRead = true

    expect((await ctx.jobs.wait(monitor, 1_500, agent)).status).toBe('failed')
    expect(ctx.jobs.get(target, agent).status).toBe('running')
    expect(producer.cancelCalls).toBe(0)
    producer.throwOnRead = false
    producer.finish()
  })

  it('rejects non-streaming jobs without leaving a monitor record behind', async () => {
    const { ctx, agent } = await harness()
    let finish!: (outcome: JobOutcome) => void
    const done = new Promise<JobOutcome>((resolve) => { finish = resolve })
    const target = ctx.jobs.start({
      kind: 'subagent',
      label: 'non-stream task',
      owner: agent,
      run: () => ({ cancel: () => finish({ status: 'killed' }), done }),
    })
    const result = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: CallId('monitor-non-stream'),
      name: 'job_monitor',
      arguments: { job_id: target, description: 'not possible' },
      agent,
    })
    expect(result.isError).toBe(true)
    expect(ctx.jobs.list(agent).filter(job => job.kind === 'monitor')).toHaveLength(0)
    finish({ status: 'completed' })
  })

  it('enforces the original owner fence when subscribing by job id', async () => {
    const { ctx, agent } = await harness()
    const producer = new StreamProducer()
    const target = startStream(ctx, agent, producer)
    const otherId = SessionId('different-owner')
    const other = {
      ...agent,
      id: otherId,
      session: { id: otherId, header: { version: 0, id: otherId, createdAt: 0, cwd: process.cwd() } },
    } as unknown as Agent
    ctx.agents.register(other)

    const result = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: CallId('monitor-foreign'),
      name: 'job_monitor',
      arguments: { job_id: target, description: 'foreign job' },
      agent: other,
    })
    expect(result.isError).toBe(true)
    expect(ctx.jobs.list(other).filter(job => job.kind === 'monitor')).toHaveLength(0)
    producer.finish()
  })
})
