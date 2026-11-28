/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.core.journal;

import io.agentscope.core.agent.AgentExecutionPosition;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.state.TaskVerification;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;

/** Blocking persistence SPI; runtime integration schedules it off reactive event-loop threads. */
public interface ThreadJournal {
    int FORMAT_VERSION = 2;

    record Head(
            int formatVersion,
            long seq,
            long epoch,
            String owner,
            long leaseUntil,
            String commitId) {
        public Head {
            if (formatVersion != FORMAT_VERSION)
                throw new JournalException("Unsupported Thread journal format: " + formatVersion);
            if (seq < 0 || epoch < 0 || leaseUntil < 0)
                throw new JournalException("Invalid Thread journal head");
        }
    }

    record Writer(String owner, long epoch) {}

    /**
     * A backend-specific state replay boundary. Consumers that need historical facts, including
     * pending interactions and per-sink exports, must not infer they are represented in state.
     */
    default long replayStart() {
        return 0;
    }

    /** Hosted thread identity, or {@code null} when the backend cannot expose one. */
    default String threadId() {
        return null;
    }

    /**
     * Snapshot of a recovery view. A seed carries every recovery fact committed at or before
     * {@code asOfSeq} that a suffix-only scan cannot reconstruct: tool outcomes and
     * verifications/extensions folded into the checkpoint state, the message identities they
     * used, applied inputs, run history, and the checkpoint chain link. Facts committed after
     * {@code asOfSeq} are always read from the journal, so a seed never hides newer history.
     * Backends return {@code null} when no checkpoint proves the required coverage, and readers
     * fall back to a full replay.
     */
    record ProjectionSeed(
            long asOfSeq,
            String stateJson,
            Set<String> uncertainToolCalls,
            Set<String> activeRuns,
            String positionTurnId,
            String positionRunId,
            AgentExecutionPosition executionPosition,
            String checkpointId,
            Map<String, ToolResultBlock> results,
            List<TaskVerification> verifications,
            List<JournalEvent> extensions,
            Set<String> messageIds,
            Set<String> appliedInputs,
            String latestRun,
            String latestTurn,
            boolean finished,
            String interruptedStep,
            String pendingModelCallId,
            String modelAnswerId,
            String modelContinuationTurnId) {
        public ProjectionSeed {
            uncertainToolCalls =
                    uncertainToolCalls == null ? Set.of() : Set.copyOf(uncertainToolCalls);
            activeRuns = activeRuns == null ? Set.of() : Set.copyOf(activeRuns);
            results = results == null ? Map.of() : java.util.Map.copyOf(results);
            verifications = verifications == null ? List.of() : List.copyOf(verifications);
            extensions = extensions == null ? List.of() : List.copyOf(extensions);
            messageIds = messageIds == null ? Set.of() : Set.copyOf(messageIds);
            appliedInputs = appliedInputs == null ? Set.of() : Set.copyOf(appliedInputs);
        }
    }

    /** Recovery seed at the latest checkpoint, or {@code null} to replay from the beginning. */
    default ProjectionSeed projectionSeed() {
        return projectionSeed(head().seq());
    }

    /**
     * Events appended since the latest checkpoint known at open time. The periodic checkpoint
     * uses it so the checkpoint interval stays bounded across invocations, not just within one.
     */
    default long checkpointGap() {
        return 0;
    }

    /**
     * Recovery seed at the newest checkpoint at or before {@code fromSeq}. Callers pass the point
     * they want to resume from — head for a normal resume, a fork point for a branch.
     */
    default ProjectionSeed projectionSeed(long fromSeq) {
        return null;
    }

    default long exportCursor(String sink) {
        return 0;
    }

    default void advanceExportCursor(String sink, long expected, long next) {
        throw new UnsupportedOperationException("Export cursor persistence not implemented");
    }

    /**
     * Best-effort presentation hook, called for every appended event before it is committed.
     *
     * <p>Hosted journals use this to forward streaming deltas to clients at the model's own pace
     * instead of waiting for the next batched commit. Delivery may repeat or be lost and must never
     * change committed state; implementations should swallow their own failures.
     */
    default void live(JournalEvent event) {}

    /**
     * Sequence of the most recent committed state baseline, or {@code 0} when the backend cannot
     * expose one. Recovery may start replaying here instead of at the beginning of the journal.
     */
    default long lastCheckpointSeq() {
        return 0;
    }

    /**
     * Sequence of the most recent {@code run/start}, or {@code 0} when unknown. Recovery must
     * include it so active runs and uncertain tool calls stay visible.
     */
    default long lastRunStartSeq() {
        return 0;
    }

    /** Stable-prefix scan. Implementations can avoid repeatedly traversing a commit chain. */
    default Iterable<JournalEvent> scan(long after, long through) {
        return scanRange(after, through, this::readAfter);
    }

    /**
     * Reads event envelopes and ordinary payloads without loading external state bodies. Backends
     * that externalize state may return references for those bodies; use {@link #materialize} when
     * a consumer needs the original payload. Existing backends return their ordinary events.
     */
    default List<JournalEvent> readMetadataAfter(long after, int limit) {
        return readAfter(after, limit);
    }

    /** Scans the same stable prefix as {@link #scan}, retaining events whose state is external. */
    default Iterable<JournalEvent> scanMetadata(long after, long through) {
        return scanRange(after, through, this::readMetadataAfter);
    }

    /** Resolves and validates an external state body without changing the event's identity. */
    default JournalEvent materialize(JournalEvent event) {
        return event;
    }

    private static Iterable<JournalEvent> scanRange(
            long after, long through, BiFunction<Long, Integer, List<JournalEvent>> read) {
        return () ->
                new Iterator<JournalEvent>() {
                    long cursor = after;
                    Iterator<JournalEvent> batch = List.<JournalEvent>of().iterator();

                    public boolean hasNext() {
                        if (cursor >= through) return false;
                        if (!batch.hasNext())
                            batch =
                                    read.apply(cursor, (int) Math.min(256, through - cursor))
                                            .iterator();
                        if (!batch.hasNext())
                            throw new JournalException("Missing committed prefix");
                        return true;
                    }

                    public JournalEvent next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        var event = batch.next();
                        cursor = event.seq();
                        return event;
                    }
                };
    }

    Head head();

    Writer acquire(String owner, Duration lease);

    /**
     * Permanently fences this writer identity, including an acquisition that has not started yet.
     * Other writer identities may continue this Thread. Returns a stable prefix for the sealed
     * writer; implementations must serialize sealing against acquisition and commit.
     */
    default Head sealWriter(String owner) {
        throw new UnsupportedOperationException("This Thread backend cannot seal a writer");
    }

    void renew(Writer writer, Duration lease);

    void release(Writer writer);

    Head commit(Writer writer, String batchId, long expectedSeq, List<JournalEvent> events);

    List<JournalEvent> readAfter(long after, int limit);
}
