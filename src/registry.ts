/** Monitor-capable process-local implementation of the DSH JobRegistry seam. */
import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import type { Config as LocalJobsConfig } from '@deepseek-ai/dsh-jobs-local'
import type { JobHooks, JobId, JobOutcome, JobStart } from '@deepseek-ai/dsh-jobs'
import { BoundedTextQueue, LineFramer } from './shared.js'

export const name = 'monitor-jobs'

/** Stable cross-entrypoint marker for the optional output-subscription capability. */
export const MONITORABLE_JOBS = Symbol.for('dsh-tool-monitor.monitorable-jobs')

export interface OutputObserver {
  onLines(lines: readonly string[]): void
  onDone(outcome: JobOutcome): void
}

export interface OutputSubscription {
  close(): void
}

export interface MonitorableJobs {
  readonly [MONITORABLE_JOBS]: true
  subscribeOutput(id: JobId, caller: Agent | undefined, observer: OutputObserver): OutputSubscription
}

export function isMonitorableJobs(value: unknown): value is MonitorableJobs {
  return typeof value === 'object'
    && value !== null
    && (value as Partial<MonitorableJobs>)[MONITORABLE_JOBS] === true
    && typeof (value as Partial<MonitorableJobs>).subscribeOutput === 'function'
}

interface Subscriber extends OutputObserver {
  closed: boolean
}

/** Lazily owns the producer's consuming cursor only while at least one observer exists. */
class OutputTee {
  private readonly unread: BoundedTextQueue
  private readonly subscribers = new Set<Subscriber>()
  private framer = new LineFramer()
  private timer: ReturnType<typeof setInterval> | undefined
  private terminal: JobOutcome | undefined
  private broken = false

  constructor(
    private readonly source: () => string,
    maxBufferedBytes: number,
    private readonly pollIntervalMs: number,
    private readonly onReadError: (error: unknown) => void,
  ) {
    this.unread = new BoundedTextQueue(maxBufferedBytes)
  }

  readForJobOutput(): string {
    const buffered = this.unread.take()
    if (buffered.length > 0) return buffered
    if (this.subscribers.size > 0 && !this.broken) return ''
    return this.source()
  }

  subscribe(observer: OutputObserver): OutputSubscription {
    if (this.terminal !== undefined) throw new Error('cannot monitor a job that has already finished')
    if (this.broken) throw new Error('cannot monitor a job whose output stream is unavailable')

    const subscriber: Subscriber = { ...observer, closed: false }
    if (this.subscribers.size === 0) {
      // Preserve output that predates the subscription for job_output only.
      this.unread.push(this.source())
      this.framer = new LineFramer()
    }
    this.subscribers.add(subscriber)
    this.startPolling()

    return {
      close: () => {
        if (subscriber.closed) return
        subscriber.closed = true
        this.subscribers.delete(subscriber)
        if (this.subscribers.size === 0) this.stopPolling()
      },
    }
  }

  finish(outcome: JobOutcome): void {
    if (this.terminal !== undefined) return
    this.terminal = outcome
    if (this.subscribers.size > 0 && !this.broken) {
      this.drain(true)
      this.broadcast(this.framer.flush())
    }
    this.stopPolling()
    const subscribers = [...this.subscribers]
    this.subscribers.clear()
    for (const subscriber of subscribers) {
      if (subscriber.closed) continue
      subscriber.closed = true
      try {
        subscriber.onDone(outcome)
      } catch (error: unknown) {
        this.onReadError(error)
      }
    }
  }

  private startPolling(): void {
    if (this.timer !== undefined) return
    this.timer = setInterval(() => this.drain(true), this.pollIntervalMs)
  }

  private stopPolling(): void {
    if (this.timer !== undefined) clearInterval(this.timer)
    this.timer = undefined
    // A later subscription starts at a new byte boundary and must not inherit
    // an incomplete line from a monitor that was explicitly stopped.
    if (this.terminal === undefined) this.framer = new LineFramer()
  }

  private drain(broadcast: boolean): void {
    try {
      const chunk = this.source()
      if (chunk.length === 0) return
      this.unread.push(chunk)
      if (broadcast) this.broadcast(this.framer.push(chunk))
    } catch (error: unknown) {
      this.broken = true
      this.stopPolling()
      const outcome: JobOutcome = { status: 'failed', detail: `output read failed: ${String(error)}` }
      const subscribers = [...this.subscribers]
      this.subscribers.clear()
      for (const subscriber of subscribers) {
        if (subscriber.closed) continue
        subscriber.closed = true
        try { subscriber.onDone(outcome) } catch (deliveryError: unknown) { this.onReadError(deliveryError) }
      }
      this.onReadError(error)
    }
  }

  private broadcast(lines: readonly string[]): void {
    if (lines.length === 0) return
    for (const subscriber of [...this.subscribers]) {
      if (subscriber.closed) continue
      try {
        subscriber.onLines(lines)
      } catch (error: unknown) {
        this.onReadError(error)
      }
    }
  }
}

/**
 * Official LocalJobRegistry plus an opt-in tee around stream producers.
 * Every lifecycle and authorization operation remains owned by the superclass.
 */
export class MonitorableJobRegistry extends LocalJobRegistry implements MonitorableJobs {
  readonly [MONITORABLE_JOBS] = true as const
  private readonly tees = new Map<JobId, OutputTee>()
  private readonly pollIntervalMs = 100

  constructor(ctx: Context, config: LocalJobsConfig) {
    super(ctx, config)
    this.onJobDone((snapshot) => { this.tees.delete(snapshot.id) })
  }

  override start(spec: JobStart): JobId {
    let tee: OutputTee | undefined
    const id = super.start({
      ...spec,
      run: (): JobHooks => {
        const hooks = spec.run()
        if (hooks.readOutput === undefined) return hooks

        tee = new OutputTee(
          hooks.readOutput.bind(hooks),
          spec.outputLimitBytes ?? 64_000,
          this.pollIntervalMs,
          error => this.ctx.logger.warn(`dsh-tool-monitor: output tee error: ${String(error)}`),
        )
        const currentTee = tee
        return {
          cancel: hooks.cancel.bind(hooks),
          done: hooks.done.then(
            (outcome) => {
              currentTee.finish(outcome)
              return outcome
            },
            (error: unknown) => {
              currentTee.finish({ status: 'failed', detail: String(error) })
              throw error
            },
          ),
          readOutput: () => currentTee.readForJobOutput(),
        }
      },
    })
    if (tee !== undefined) this.tees.set(id, tee)
    return id
  }

  subscribeOutput(id: JobId, caller: Agent | undefined, observer: OutputObserver): OutputSubscription {
    const snapshot = this.get(id, caller)
    if (snapshot.status !== 'running' && snapshot.status !== 'stopping') {
      throw new Error(`cannot monitor ${id}: job is ${snapshot.status}`)
    }
    const tee = this.tees.get(id)
    if (tee === undefined) throw new Error(`cannot monitor ${id}: job does not expose streaming output`)
    return tee.subscribe(observer)
  }
}

export default MonitorableJobRegistry
