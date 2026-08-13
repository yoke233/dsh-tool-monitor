import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local';
import { BoundedTextQueue, LineFramer } from './shared.js';
export const name = 'monitor-jobs';
/** Stable cross-entrypoint marker for the optional output-subscription capability. */
export const MONITORABLE_JOBS = Symbol.for('dsh-tool-monitor.monitorable-jobs');
export function isMonitorableJobs(value) {
    return typeof value === 'object'
        && value !== null
        && value[MONITORABLE_JOBS] === true
        && typeof value.subscribeOutput === 'function';
}
/** Lazily owns the producer's consuming cursor only while at least one observer exists. */
class OutputTee {
    source;
    pollIntervalMs;
    onReadError;
    unread;
    subscribers = new Set();
    framer = new LineFramer();
    timer;
    terminal;
    broken = false;
    constructor(source, maxBufferedBytes, pollIntervalMs, onReadError) {
        this.source = source;
        this.pollIntervalMs = pollIntervalMs;
        this.onReadError = onReadError;
        this.unread = new BoundedTextQueue(maxBufferedBytes);
    }
    readForJobOutput() {
        const buffered = this.unread.take();
        if (buffered.length > 0)
            return buffered;
        if (this.subscribers.size > 0 && !this.broken)
            return '';
        return this.source();
    }
    subscribe(observer) {
        if (this.terminal !== undefined)
            throw new Error('cannot monitor a job that has already finished');
        if (this.broken)
            throw new Error('cannot monitor a job whose output stream is unavailable');
        const subscriber = { ...observer, closed: false };
        if (this.subscribers.size === 0) {
            // Preserve output that predates the subscription for job_output only.
            this.unread.push(this.source());
            this.framer = new LineFramer();
        }
        this.subscribers.add(subscriber);
        this.startPolling();
        return {
            close: () => {
                if (subscriber.closed)
                    return;
                subscriber.closed = true;
                this.subscribers.delete(subscriber);
                if (this.subscribers.size === 0)
                    this.stopPolling();
            },
        };
    }
    finish(outcome) {
        if (this.terminal !== undefined)
            return;
        this.terminal = outcome;
        if (this.subscribers.size > 0 && !this.broken) {
            this.drain(true);
            this.broadcast(this.framer.flush());
        }
        this.stopPolling();
        const subscribers = [...this.subscribers];
        this.subscribers.clear();
        for (const subscriber of subscribers) {
            if (subscriber.closed)
                continue;
            subscriber.closed = true;
            try {
                subscriber.onDone(outcome);
            }
            catch (error) {
                this.onReadError(error);
            }
        }
    }
    startPolling() {
        if (this.timer !== undefined)
            return;
        this.timer = setInterval(() => this.drain(true), this.pollIntervalMs);
    }
    stopPolling() {
        if (this.timer !== undefined)
            clearInterval(this.timer);
        this.timer = undefined;
        // A later subscription starts at a new byte boundary and must not inherit
        // an incomplete line from a monitor that was explicitly stopped.
        if (this.terminal === undefined)
            this.framer = new LineFramer();
    }
    drain(broadcast) {
        try {
            const chunk = this.source();
            if (chunk.length === 0)
                return;
            this.unread.push(chunk);
            if (broadcast)
                this.broadcast(this.framer.push(chunk));
        }
        catch (error) {
            this.broken = true;
            this.stopPolling();
            const outcome = { status: 'failed', detail: `output read failed: ${String(error)}` };
            const subscribers = [...this.subscribers];
            this.subscribers.clear();
            for (const subscriber of subscribers) {
                if (subscriber.closed)
                    continue;
                subscriber.closed = true;
                try {
                    subscriber.onDone(outcome);
                }
                catch (deliveryError) {
                    this.onReadError(deliveryError);
                }
            }
            this.onReadError(error);
        }
    }
    broadcast(lines) {
        if (lines.length === 0)
            return;
        for (const subscriber of [...this.subscribers]) {
            if (subscriber.closed)
                continue;
            try {
                subscriber.onLines(lines);
            }
            catch (error) {
                this.onReadError(error);
            }
        }
    }
}
/**
 * Official LocalJobRegistry plus an opt-in tee around stream producers.
 * Every lifecycle and authorization operation remains owned by the superclass.
 */
export class MonitorableJobRegistry extends LocalJobRegistry {
    [MONITORABLE_JOBS] = true;
    tees = new Map();
    pollIntervalMs = 100;
    constructor(ctx, config) {
        super(ctx, config);
        this.onJobDone((snapshot) => { this.tees.delete(snapshot.id); });
    }
    start(spec) {
        let tee;
        const id = super.start({
            ...spec,
            run: () => {
                const hooks = spec.run();
                if (hooks.readOutput === undefined)
                    return hooks;
                tee = new OutputTee(hooks.readOutput.bind(hooks), spec.outputLimitBytes ?? 64_000, this.pollIntervalMs, error => this.ctx.logger.warn(`dsh-tool-monitor: output tee error: ${String(error)}`));
                const currentTee = tee;
                return {
                    cancel: hooks.cancel.bind(hooks),
                    done: hooks.done.then((outcome) => {
                        currentTee.finish(outcome);
                        return outcome;
                    }, (error) => {
                        currentTee.finish({ status: 'failed', detail: String(error) });
                        throw error;
                    }),
                    readOutput: () => currentTee.readForJobOutput(),
                };
            },
        });
        if (tee !== undefined)
            this.tees.set(id, tee);
        return id;
    }
    subscribeOutput(id, caller, observer) {
        const snapshot = this.get(id, caller);
        if (snapshot.status !== 'running' && snapshot.status !== 'stopping') {
            throw new Error(`cannot monitor ${id}: job is ${snapshot.status}`);
        }
        const tee = this.tees.get(id);
        if (tee === undefined)
            throw new Error(`cannot monitor ${id}: job does not expose streaming output`);
        return tee.subscribe(observer);
    }
}
export default MonitorableJobRegistry;
