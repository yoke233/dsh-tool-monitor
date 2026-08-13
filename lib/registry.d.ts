/** Monitor-capable process-local implementation of the DSH JobRegistry seam. */
import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local';
import type { Config as LocalJobsConfig } from '@deepseek-ai/dsh-jobs-local';
import type { JobId, JobOutcome, JobStart } from '@deepseek-ai/dsh-jobs';
export declare const name = "monitor-jobs";
/** Stable cross-entrypoint marker for the optional output-subscription capability. */
export declare const MONITORABLE_JOBS: unique symbol;
export interface OutputObserver {
    onLines(lines: readonly string[]): void;
    onDone(outcome: JobOutcome): void;
}
export interface OutputSubscription {
    close(): void;
}
export interface MonitorableJobs {
    readonly [MONITORABLE_JOBS]: true;
    subscribeOutput(id: JobId, caller: Agent | undefined, observer: OutputObserver): OutputSubscription;
}
export declare function isMonitorableJobs(value: unknown): value is MonitorableJobs;
/**
 * Official LocalJobRegistry plus an opt-in tee around stream producers.
 * Every lifecycle and authorization operation remains owned by the superclass.
 */
export declare class MonitorableJobRegistry extends LocalJobRegistry implements MonitorableJobs {
    readonly [MONITORABLE_JOBS]: true;
    private readonly tees;
    private readonly pollIntervalMs;
    constructor(ctx: Context, config: LocalJobsConfig);
    start(spec: JobStart): JobId;
    subscribeOutput(id: JobId, caller: Agent | undefined, observer: OutputObserver): OutputSubscription;
}
export default MonitorableJobRegistry;
