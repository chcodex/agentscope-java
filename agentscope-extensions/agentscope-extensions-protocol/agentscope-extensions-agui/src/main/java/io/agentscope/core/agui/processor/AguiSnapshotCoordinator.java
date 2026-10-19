/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.core.agui.processor;

import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.agui.store.AguiSnapshotStore;
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates snapshot recording and hydration for {@link AguiRequestProcessor}.
 *
 * <p>The coordinator owns the {@link AguiSnapshotStore} lifecycle glue: it classifies whether an
 * input satisfies the hydrate quadruple ({@code messages empty && resume empty && snapshot store
 * enabled && snapshot exists}), manages one bounded accumulator per live run, and materializes a
 * finished run into the store's latest-only covering write.
 *
 * <p>A missing or blank {@code threadId} is rejected outright instead of falling back to a shared
 * bucket such as {@code "unknown"}: a fallback key would silently serve one thread's presentation
 * state to another thread.
 */
final class AguiSnapshotCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(AguiSnapshotCoordinator.class);

    private final AguiSnapshotStore store;
    private final int maxAccumulators;
    private final ConcurrentMap<String, AguiSnapshotAccumulator> accumulators =
            new ConcurrentHashMap<>();

    AguiSnapshotCoordinator(AguiSnapshotStore store, int maxAccumulators) {
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.maxAccumulators = Math.max(1, maxAccumulators);
    }

    AguiSnapshotStore store() {
        return store;
    }

    /**
     * Returns hydrate events when the request is a hydrate target, else {@code null}.
     *
     * <p>The hydrate quadruple is checked before any Agent wiring: {@code messages} empty, {@code
     * resume} empty, the snapshot store enabled (implied by a non-null coordinator), and a stored
     * snapshot present for the thread. A hydrate candidate with a missing or blank {@code threadId}
     * is rejected with a protocol error lifecycle instead of being served from a fallback key.
     *
     * @param input the incoming AG-UI run input
     * @param emitRunFinishedAfterError whether a legacy {@code RUN_FINISHED} follows the rejection
     *     error
     * @param unresolvedInterrupts interrupts still pending on the thread, provided by the resume
     *     coordinator
     * @return hydrate events, or {@code null} to continue the normal run flow
     */
    List<AguiEvent> tryHydrate(
            RunAgentInput input,
            boolean emitRunFinishedAfterError,
            List<AguiEvent.Interrupt> unresolvedInterrupts) {
        String threadId = input.getThreadId();
        boolean hydrateCandidate = input.getMessages().isEmpty() && !input.hasResume();
        if (!hydrateCandidate) {
            return null;
        }
        if (threadId == null || threadId.isBlank()) {
            return rejectEvents(input, "threadId is required", emitRunFinishedAfterError);
        }
        AguiThreadSnapshot snapshot = store.get(threadId);
        if (snapshot == null) {
            return null;
        }
        return AguiSnapshotHydrator.hydrateEvents(
                threadId, input.getRunId(), snapshot, unresolvedInterrupts);
    }

    /**
     * Registers a per-run accumulator for materialization.
     *
     * <p>Registry entries are removed when the run finishes; the hard cap is a bottom-line guard
     * so a leaked entry (never-subscribed stream) cannot retain whole unbounded thread transcripts.
     *
     * @param input the run input
     * @return the accumulator, or {@code null} when the threadId is missing/blank
     */
    AguiSnapshotAccumulator begin(RunAgentInput input) {
        String threadId = input.getThreadId();
        if (threadId == null || threadId.isBlank()) {
            return null;
        }
        if (accumulators.size() >= maxAccumulators) {
            logger.warn(
                    "Snapshot accumulator registry reached capacity {}, bottom-line clearing"
                            + " registry map (stale live-run records are dropped)",
                    maxAccumulators);
            accumulators.clear();
        }
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator(threadId, input.getRunId(), store.get(threadId));
        accumulators.put(threadId, accumulator);
        return accumulator;
    }

    /**
     * Finishes a run: materializes the accumulator, stores the snapshot, and removes the registry
     * entry.
     *
     * @param threadId the AG-UI thread id
     * @param accumulator the accumulator registered by {@link #begin(RunAgentInput)}
     * @param completedNormally whether the stream completed normally (not cancelled or failed)
     */
    void finishRun(
            String threadId, AguiSnapshotAccumulator accumulator, boolean completedNormally) {
        if (accumulator == null) {
            return;
        }
        if (threadId == null || !accumulators.remove(threadId, accumulator)) {
            return;
        }
        if (!completedNormally) {
            return;
        }
        if (accumulator.isRunError() || !accumulator.isRunFinished()) {
            // A failed run leaves the previous covering snapshot untouched.
            return;
        }
        try {
            store.save(threadId, accumulator.materialize());
        } catch (RuntimeException error) {
            logger.warn(
                    "Failed to save AG-UI snapshot for thread {}: {}",
                    threadId,
                    error.getMessage());
        }
    }

    private static List<AguiEvent> rejectEvents(
            RunAgentInput input, String message, boolean emitRunFinishedAfterError) {
        String threadId = input.getThreadId() == null ? "" : input.getThreadId();
        List<AguiEvent> events = new ArrayList<>();
        events.add(new AguiEvent.RunStarted(threadId, input.getRunId(), null, input));
        events.add(
                new AguiEvent.RunError(
                        threadId,
                        input.getRunId(),
                        message,
                        "INVALID_INPUT_ERROR",
                        System.currentTimeMillis(),
                        null));
        if (emitRunFinishedAfterError) {
            events.add(new AguiEvent.RunFinished(threadId, input.getRunId()));
        }
        return List.copyOf(events);
    }
}
