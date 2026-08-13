/** Model-facing job_monitor tool over MonitorableJobRegistry. */
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { boundContextSummary, createUserMessage } from '@deepseek-ai/dsh-llm'
import { JobId } from '@deepseek-ai/dsh-jobs'
import type { JobOutcome } from '@deepseek-ai/dsh-jobs'
import { defineTool } from '@deepseek-ai/dsh-tools'
import type { GenericCallView } from '@deepseek-ai/dsh-tools'
import type {} from '@deepseek-ai/dsh-system-prompt'
import { isMonitorableJobs } from './registry.js'
import { BoundedTextQueue, retainUtf8Tail } from './shared.js'

export const name = 'tool-monitor'
export const inject = ['tools', 'jobs', 'systemPrompt']

declare module '@deepseek-ai/dsh-jobs' {
  interface JobKindMap { monitor: 'monitor' }
}

export interface Config {
  batchWindowMs?: number
  maxBatchBytes?: number
  maxBufferedBytes?: number
  maxConsecutiveWakes?: number
}

export const Config: z<Config> = z.object({
  batchWindowMs: z.number().min(25).default(500),
  maxBatchBytes: z.number().min(256).default(8_192),
  maxBufferedBytes: z.number().min(1_024).default(64_000),
  maxConsecutiveWakes: z.number().min(1).default(20),
})

interface MonitorArgs {
  job_id: string
  description: string
  pattern?: string
  case_sensitive?: boolean
  timeout_ms?: number
}

function validateConfig(config: Required<Config>): void {
  for (const [key, value] of Object.entries(config)) {
    if (!Number.isSafeInteger(value)) throw new Error(`tool-monitor: ${key} must be a whole number`)
  }
}

function validateArgs(args: MonitorArgs): RegExp | undefined {
  if (args.job_id.trim().length === 0) throw new Error('invalid job_id: expected a non-empty string')
  if (args.description.trim().length === 0) throw new Error('invalid description: expected a non-empty string')
  if (args.timeout_ms !== undefined && (!Number.isSafeInteger(args.timeout_ms) || args.timeout_ms <= 0)) {
    throw new Error(`invalid timeout_ms: expected a positive whole number, got ${JSON.stringify(args.timeout_ms)}`)
  }
  if (args.pattern === undefined) return undefined
  if (args.pattern.length === 0) throw new Error('invalid pattern: expected a non-empty regular expression')
  try {
    return new RegExp(args.pattern, args.case_sensitive === true ? '' : 'i')
  } catch (error: unknown) {
    throw new Error(`invalid pattern: ${String(error)}`)
  }
}

function presentMonitor(args: MonitorArgs): GenericCallView {
  return {
    card: 'generic',
    title: `Monitor ${args.job_id}: ${args.description}`,
    kind: 'execute',
    rawInput: args.pattern ?? '(all output lines)',
    content: [{ type: 'text', text: 'Subscribe to existing background job output' }],
  }
}

function targetFinishedOutcome(targetId: string, outcome: JobOutcome): JobOutcome {
  return {
    status: 'completed',
    detail: `target ${targetId} ${outcome.status}${outcome.detail === undefined ? '' : ` (${outcome.detail})`}`,
  }
}

/** Register a subscription-only Monitor. No shell command or business process is started. */
export function apply(ctx: Context, input: Config = {}): void {
  const config: Required<Config> = {
    batchWindowMs: input.batchWindowMs ?? 500,
    maxBatchBytes: input.maxBatchBytes ?? 8_192,
    maxBufferedBytes: input.maxBufferedBytes ?? 64_000,
    maxConsecutiveWakes: input.maxConsecutiveWakes ?? 20,
  }
  validateConfig(config)
  const jobs = ctx.jobs
  if (!isMonitorableJobs(jobs)) {
    throw new Error('tool-monitor requires dsh-tool-monitor/registry as the ctx.jobs provider')
  }

  const spentWakes = new WeakMap<Agent, number>()
  ctx.on('agent/inbox/claimed', ({ agent, message }) => {
    if (message.source.kind === 'user') spentWakes.delete(agent)
  })

  ctx.systemPrompt.section({
    name: 'tool:monitor',
    order: 107,
    text: 'Use job_monitor to subscribe to actionable output from an existing background job without executing its command again. Pass the job id returned by bash or pwsh with run_in_background, and use a selective regular expression when only errors or state changes matter. The monitor ends with its target and can be stopped independently with job_kill.',
  })

  ctx.tools.register(defineTool({
    name: 'job_monitor',
    description: 'Subscribe to output lines from an existing streaming background job without running its command again. '
      + 'Matching lines are delivered as in-session events while the target keeps running. The returned monitor job appears in job_list, '
      + 'its matched output can be read with job_output, and job_kill stops only the subscription, not the target job.',
    parameters: {
      job_id: { type: 'string', required: true, description: 'Existing background job id returned by bash or pwsh with run_in_background.' },
      description: { type: 'string', required: true, description: 'Short description shown in Monitor notifications.' },
      pattern: { type: 'string', description: 'Optional JavaScript regular expression matched independently against each complete output line.' },
      case_sensitive: { type: 'boolean', description: 'Make pattern matching case-sensitive. Defaults to false.' },
      timeout_ms: { type: 'number', description: 'Optional subscription lifetime in milliseconds. By default it follows the target job.' },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          jobId: { type: 'string', required: true },
          targetJobId: { type: 'string', required: true },
        },
      },
      render: (_args, value) => [{
        type: 'text',
        text: `started monitor ${value.jobId} for existing job ${value.targetJobId}`,
      }],
    },
    execute(args, exec) {
      const matcher = validateArgs(args)
      const owner = exec.agent
      if (owner === undefined) throw new Error('job_monitor requires an agent-owned session')
      if (exec.signal.aborted) throw new Error('job_monitor start aborted')
      const targetId = JobId(args.job_id)
      let monitorId: JobId | undefined

      monitorId = ctx.jobs.start({
        kind: 'monitor',
        label: `${args.description} ← ${targetId}`,
        owner,
        outputLimitBytes: config.maxBufferedBytes,
        run: () => {
          const output = new BoundedTextQueue(config.maxBufferedBytes)
          let pending: string[] = []
          let deliveryTimer: ReturnType<typeof setTimeout> | undefined
          let timeoutTimer: ReturnType<typeof setTimeout> | undefined
          let stopped = false
          let settled = false
          let settle!: (outcome: JobOutcome) => void
          const done = new Promise<JobOutcome>((resolve) => { settle = resolve })

          const flushNotice = (): void => {
            deliveryTimer = undefined
            if (stopped || pending.length === 0 || monitorId === undefined) return
            const lines = pending
            pending = []
            const body = retainUtf8Tail(lines.join('\n'), config.maxBatchBytes)
            const message = createUserMessage({
              content: [{ type: 'text', text: `monitor ${monitorId} (${args.description}) observed ${targetId}:\n${body}` }],
              source: {
                kind: 'plugin',
                plugin: 'tool-monitor',
                form: 'notice',
                summary: boundContextSummary(`monitor ${args.description}: ${lines.at(-1) ?? ''}`),
              },
            })
            const spent = spentWakes.get(owner) ?? 0
            try {
              if (owner.status === 'idle' && spent < config.maxConsecutiveWakes) {
                spentWakes.set(owner, spent + 1)
                owner.followup(message)
              } else {
                owner.inject(message)
              }
            } catch (error: unknown) {
              ctx.logger.warn(`tool-monitor: event delivery failed for ${monitorId}: ${String(error)}`)
            }
          }

          const finish = (outcome: JobOutcome, discardPending = false): void => {
            if (settled) return
            settled = true
            if (timeoutTimer !== undefined) clearTimeout(timeoutTimer)
            if (deliveryTimer !== undefined) clearTimeout(deliveryTimer)
            if (discardPending) pending = []
            else flushNotice()
            settle(outcome)
          }

          const subscription = jobs.subscribeOutput(targetId, owner, {
            onLines(lines) {
              if (stopped) return
              const matched = matcher === undefined ? [...lines] : lines.filter(line => matcher.test(line))
              if (matched.length === 0) return
              const text = `${matched.join('\n')}\n`
              output.push(text)
              pending.push(...matched)
              if (deliveryTimer === undefined) deliveryTimer = setTimeout(flushNotice, config.batchWindowMs)
            },
            onDone(outcome) {
              finish(outcome.status === 'failed'
                ? { status: 'failed', detail: `${targetId} output subscription failed: ${outcome.detail ?? 'unknown error'}` }
                : targetFinishedOutcome(targetId, outcome))
            },
          })

          if (args.timeout_ms !== undefined) {
            timeoutTimer = setTimeout(() => {
              subscription.close()
              finish({ status: 'killed', detail: `monitor timeout after ${args.timeout_ms}ms` })
            }, args.timeout_ms)
          }

          return {
            cancel: () => {
              if (stopped) return
              stopped = true
              subscription.close()
              finish({ status: 'killed', detail: 'monitor stopped' }, true)
            },
            done,
            readOutput: () => output.take(),
          }
        },
      })

      return Promise.resolve({ jobId: monitorId, targetJobId: targetId })
    },
    presentCall: presentMonitor,
  }))
}
