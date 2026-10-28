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

import io.agentscope.core.agui.adapter.AguiAdapterConfig;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.event.AguiFramedEvent;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.session.SessionEvent;
import io.agentscope.core.session.SessionLog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

/**
 * Replays committed {@link SessionLog} events after a recovery cursor.
 *
 * <p>This is a pure observer: it never starts or resumes an Agent run and never builds a message
 * snapshot. The application supplies the conversation history and a cursor that is a presentation
 * boundary. If the run is still active, the stream polls the session head and reads new commits
 * until the run ends; if it has already ended, the stream drains the tail and finishes.
 *
 * <p>The wait between polls is scheduled through Reactor, so an idle attach does not hold a worker
 * thread. The blocking store reads run briefly on {@code boundedElastic}.
 */
final class AguiSessionLogAttach {

    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(200);
    private static final int BATCH_SIZE = 256;

    private AguiSessionLogAttach() {}

    static Flux<AguiFramedEvent> attach(
            RunAgentInput input,
            SessionLog log,
            long after,
            String executionRunId,
            AguiAdapterConfig config) {
        return attach(input, log, after, executionRunId, config, DEFAULT_POLL_INTERVAL);
    }

    static Flux<AguiFramedEvent> attach(
            RunAgentInput input,
            SessionLog log,
            long after,
            String executionRunId,
            AguiAdapterConfig config,
            Duration pollInterval) {
        return Flux.<AguiFramedEvent>create(
                sink -> runAttach(sink, input, log, after, executionRunId, config, pollInterval));
    }

    private static void runAttach(
            FluxSink<AguiFramedEvent> sink,
            RunAgentInput input,
            SessionLog log,
            long after,
            String executionRunId,
            AguiAdapterConfig config,
            Duration pollInterval) {
        try {
            long head = log.head().seq();
            if (after > head) {
                emitFrames(
                        sink,
                        errorFrames(
                                input,
                                -1,
                                "Session log cursor " + after + " is ahead of " + head,
                                "INVALID_INPUT_ERROR",
                                config,
                                true));
                return;
            }
        } catch (RuntimeException error) {
            emitFrames(
                    sink,
                    errorFrames(input, -1, errorMessage(error), "SESSION_LOG_ERROR", config, true));
            return;
        }

        Duration poll = pollInterval != null ? pollInterval : DEFAULT_POLL_INTERVAL;
        Duration timeout =
                config.getRunTimeout() != null ? config.getRunTimeout() : Duration.ofMinutes(10);
        long deadline = System.nanoTime() + timeout.toNanos();
        AttachState state =
                new AttachState(
                        after,
                        new AguiSessionPresentation(
                                input.getThreadId(), input.getRunId(), config, input));

        sink.next(
                AguiFramedEvent.uncursored(
                        new AguiEvent.RunStarted(
                                input.getThreadId(), input.getRunId(), null, input)));

        AtomicReference<Disposable> pending = new AtomicReference<>();
        sink.onDispose(
                () -> {
                    Disposable disposable = pending.getAndSet(null);
                    if (disposable != null) {
                        disposable.dispose();
                    }
                });

        AtomicReference<Runnable> pollRef = new AtomicReference<>();
        Runnable pollTask =
                () -> {
                    if (sink.isCancelled()) {
                        return;
                    }
                    pending.set(
                            Schedulers.boundedElastic()
                                    .schedule(
                                            () -> {
                                                if (sink.isCancelled()) {
                                                    return;
                                                }
                                                PollOutcome outcome;
                                                try {
                                                    outcome =
                                                            pollOnce(
                                                                    log,
                                                                    state,
                                                                    input,
                                                                    executionRunId,
                                                                    config,
                                                                    deadline);
                                                } catch (RuntimeException error) {
                                                    outcome =
                                                            new PollOutcome(
                                                                    errorFrames(
                                                                            input,
                                                                            state.cursor,
                                                                            errorMessage(error),
                                                                            "SESSION_LOG_ERROR",
                                                                            config,
                                                                            false),
                                                                    true);
                                                }
                                                for (AguiFramedEvent frame : outcome.frames()) {
                                                    if (sink.isCancelled()) {
                                                        return;
                                                    }
                                                    sink.next(frame);
                                                }
                                                if (outcome.terminal()) {
                                                    sink.complete();
                                                    return;
                                                }
                                                long delay =
                                                        outcome.frames().isEmpty()
                                                                ? Math.max(0L, poll.toMillis())
                                                                : 0L;
                                                pending.set(
                                                        Schedulers.parallel()
                                                                .schedule(
                                                                        pollRef.get(),
                                                                        delay,
                                                                        TimeUnit.MILLISECONDS));
                                            }));
                };
        pollRef.set(pollTask);
        pending.set(Schedulers.parallel().schedule(pollTask, 0, TimeUnit.MILLISECONDS));
    }

    private static PollOutcome pollOnce(
            SessionLog log,
            AttachState state,
            RunAgentInput input,
            String executionRunId,
            AguiAdapterConfig config,
            long deadline) {
        SessionLog.Head head = log.head();
        if (head.seq() <= state.cursor) {
            if (!isActive(head, executionRunId)) {
                return new PollOutcome(
                        finishFrames(input, state.presentation, state.cursor, null, config), true);
            }
            if (System.nanoTime() >= deadline) {
                return new PollOutcome(
                        errorFrames(
                                input,
                                state.cursor,
                                "Session log observation timed out",
                                "TIMEOUT_ERROR",
                                config,
                                false),
                        true);
            }
            return new PollOutcome(List.of(), false);
        }

        List<SessionEvent> batch = log.readAfter(state.cursor, BATCH_SIZE);
        if (batch.isEmpty()) {
            if (!isActive(head, executionRunId)) {
                return new PollOutcome(
                        finishFrames(input, state.presentation, state.cursor, null, config), true);
            }
            return new PollOutcome(List.of(), false);
        }

        List<AguiFramedEvent> frames = new ArrayList<>();
        for (SessionEvent event : batch) {
            state.cursor = event.seq();
            boolean target =
                    executionRunId == null || executionRunId.equals(event.executionRunId());
            if (!target) {
                continue;
            }
            frames.addAll(frames(state.presentation.apply(event), event.seq()));
            if ("run/end".equals(event.type())) {
                if (executionRunId != null && executionRunId.equals(event.executionRunId())) {
                    frames.addAll(
                            finishFrames(input, state.presentation, state.cursor, event, config));
                    return new PollOutcome(frames, true);
                }
                if (executionRunId == null && state.presentation.activeRuns().isEmpty()) {
                    frames.addAll(
                            finishFrames(input, state.presentation, state.cursor, event, config));
                    return new PollOutcome(frames, true);
                }
            }
        }
        return new PollOutcome(frames, false);
    }

    private static boolean isActive(SessionLog.Head head, String executionRunId) {
        if (head.owner() == null || head.leaseUntil() <= System.currentTimeMillis()) {
            return false;
        }
        return executionRunId == null || executionRunId.equals(head.owner());
    }

    private static List<AguiFramedEvent> finishFrames(
            RunAgentInput input,
            AguiSessionPresentation presentation,
            long cursor,
            SessionEvent runEnd,
            AguiAdapterConfig config) {
        List<AguiEvent.Interrupt> interrupts = presentation.interrupts();
        List<AguiFramedEvent> frames = new ArrayList<>();
        if (runEnd != null && "failed".equalsIgnoreCase(status(runEnd))) {
            String reason = reason(runEnd);
            frames.add(
                    new AguiFramedEvent(
                            cursor,
                            new AguiEvent.RunError(
                                    input.getThreadId(),
                                    input.getRunId(),
                                    reason != null ? reason : "Agent run failed",
                                    "INTERNAL_ERROR",
                                    System.currentTimeMillis(),
                                    null)));
            if (config.isEmitRunFinishedAfterError()) {
                frames.add(
                        new AguiFramedEvent(
                                cursor,
                                new AguiEvent.RunFinished(input.getThreadId(), input.getRunId())));
            }
            return frames;
        }

        AguiEvent.RunFinishedOutcome outcome =
                interrupts.isEmpty() ? null : new AguiEvent.RunFinishedInterruptOutcome(interrupts);
        frames.add(
                new AguiFramedEvent(
                        cursor,
                        new AguiEvent.RunFinished(
                                input.getThreadId(), input.getRunId(), null, outcome)));
        return frames;
    }

    private static List<AguiFramedEvent> errorFrames(
            RunAgentInput input,
            long cursor,
            String message,
            String code,
            AguiAdapterConfig config,
            boolean includeRunStarted) {
        List<AguiFramedEvent> frames = new ArrayList<>();
        if (includeRunStarted) {
            frames.add(
                    AguiFramedEvent.uncursored(
                            new AguiEvent.RunStarted(
                                    input.getThreadId(), input.getRunId(), null, input)));
        }
        frames.add(
                new AguiFramedEvent(
                        cursor,
                        new AguiEvent.RunError(
                                input.getThreadId(),
                                input.getRunId(),
                                message,
                                code,
                                System.currentTimeMillis(),
                                null)));
        if (config.isEmitRunFinishedAfterError()) {
            frames.add(
                    new AguiFramedEvent(
                            cursor,
                            new AguiEvent.RunFinished(input.getThreadId(), input.getRunId())));
        }
        return frames;
    }

    private static List<AguiFramedEvent> frames(List<AguiEvent> events, long cursor) {
        List<AguiFramedEvent> frames = new ArrayList<>(events.size());
        for (int i = 0; i < events.size(); i++) {
            long seq = i == events.size() - 1 ? cursor : -1;
            frames.add(new AguiFramedEvent(seq, events.get(i)));
        }
        return frames;
    }

    private static void emitFrames(FluxSink<AguiFramedEvent> sink, List<AguiFramedEvent> frames) {
        for (AguiFramedEvent frame : frames) {
            sink.next(frame);
        }
        sink.complete();
    }

    private static String status(SessionEvent event) {
        Object value = event.data().get("status");
        return value != null ? String.valueOf(value) : null;
    }

    private static String reason(SessionEvent event) {
        Object value = event.data().get("reason");
        return value != null ? String.valueOf(value) : null;
    }

    private static String errorMessage(Throwable error) {
        return error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
    }

    private record PollOutcome(List<AguiFramedEvent> frames, boolean terminal) {}

    private static final class AttachState {
        private long cursor;
        private final AguiSessionPresentation presentation;

        private AttachState(long cursor, AguiSessionPresentation presentation) {
            this.cursor = cursor;
            this.presentation = presentation;
        }
    }
}
