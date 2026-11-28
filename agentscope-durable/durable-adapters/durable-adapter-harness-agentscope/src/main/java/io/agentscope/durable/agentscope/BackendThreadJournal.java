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

package io.agentscope.durable.agentscope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.agentscope.core.agent.RunOutcome;
import io.agentscope.core.journal.JournalEvent;
import io.agentscope.core.journal.JournalEventTypes;
import io.agentscope.core.journal.ThreadJournal;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.state.TaskVerification;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.durable.DurableException;
import io.agentscope.durable.DurableJson;
import io.agentscope.durable.DurableMessages.Input;
import io.agentscope.durable.DurableMessages.Message;
import io.agentscope.durable.DurableTypes.AppliedResult;
import io.agentscope.durable.DurableTypes.Commit;
import io.agentscope.durable.DurableTypes.EventInput;
import io.agentscope.durable.DurableTypes.JournalAppend;
import io.agentscope.durable.DurableTypes.LiveEventInput;
import io.agentscope.durable.DurableTypes.RunChange;
import io.agentscope.durable.DurableTypes.RunStatus;
import io.agentscope.durable.DurableTypes.RunStepRef;
import io.agentscope.durable.DurableTypes.StepChange;
import io.agentscope.durable.DurableTypes.StepPhase;
import io.agentscope.durable.DurableTypes.StepStatus;
import io.agentscope.durable.DurableTypes.TokenUsage;
import io.agentscope.durable.DurableTypes.ToolCall;
import io.agentscope.durable.DurableTypes.TurnStatus;
import io.agentscope.durable.DurableTypes.VerificationView;
import io.agentscope.durable.DurableTypes.WaitCondition;
import io.agentscope.durable.HarnessContext;
import io.agentscope.durable.ToolBinding;
import io.agentscope.durable.codec.agentscope.AgentScopeCheckpointCodec;
import io.agentscope.durable.codec.agentscope.DurableEventSchemas;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Native journal and durable effects share one fenced backend transaction. No second scheduler. */
final class BackendThreadJournal implements ThreadJournal {
    private final HarnessContext context;
    private final Map<String, String> executors;

    /**
     * Local export watermark per sink. The Durable journal is the source of truth and a fresh
     * journal instance replays the committed prefix, so the watermark stays in memory: a lost ACK
     * repeats events and every sink de-duplicates by event id.
     */
    private final Map<String, Long> exportCursors = new HashMap<>();

    private final Map<String, ToolBinding> bindings;
    private final String initialInputId;
    private final JsonNode initialInput;
    private final Set<String> sealed = new HashSet<>();
    private final List<String> signalsToApply = new ArrayList<>();

    /**
     * Result metadata from the native event stream, waiting for the recorder's next commit. It is
     * published as one {@code x.result_metadata} public event, not as a native journal event: the
     * value describes a result, not Agent state, and replay reads it back from the public stream.
     */
    private final Map<String, QueuedResultMetadata> queuedResultMetadata = new LinkedHashMap<>();

    private record QueuedResultMetadata(String runId, Map<String, Object> values) {}

    private String owner;

    /** Concurrent: the model stream records chunks while a commit may freeze the same call. */
    /** Events since the latest checkpoint seen while replaying; seed for the periodic check. */
    private long checkpointGap;

    private final Map<String, ModelCapture> captures =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Recovery seed is fixed at construction; later appends are covered by scanning to head().
     */
    private ProjectionSeed projectionCache;

    private long projectionCacheFrom = -1;

    private ObjectNode cached;

    /**
     * Journal sequence covered by the checkpoint this invocation restores from. History before it
     * is already reflected in the native state, so replay starts there instead of at the beginning
     * of the thread.
     */
    private long baseSeq;

    private AgentScopeMessageEvents messages = new AgentScopeMessageEvents();

    /**
     * Presentation-only state for streaming deltas. It mirrors the committed state machine on the
     * same event sequence, so live frames carry the same message ids as the committed ones.
     */
    private final AgentScopeMessageEvents liveMessages = new AgentScopeMessageEvents();

    /** Guards {@link #liveMessages} only; it must never wait on the commit monitor. */
    private final Object liveLock = new Object();

    private AgentScopeToolEvents toolEvents = new AgentScopeToolEvents();

    BackendThreadJournal(HarnessContext context, Map<String, String> executors) {
        this(context, executors, Map.of());
    }

    BackendThreadJournal(
            HarnessContext context,
            Map<String, String> executors,
            Map<String, ToolBinding> bindings) {
        this.bindings = bindings;
        DurableEventSchemas.ensureRegistered();
        this.context = context;
        this.executors = executors;
        var frame = context.frameWithCheckpoint();
        initialInputId = frame.turn().inputMessageId();
        initialInput = frame.input();
        var checkpoint = frame.checkpoint();
        if (!checkpoint.isNull()
                && !AgentScopeCheckpointCodec.STATE_VERSION.equals(
                        checkpoint.path("format").asText()))
            throw new DurableException(
                    DurableException.Code.INCOMPATIBLE, "Unknown native checkpoint format");
        cached =
                (ObjectNode)
                        DurableJson.value(
                                Map.of("format", AgentScopeCheckpointCodec.STATE_VERSION));
        cached.putArray("events");
        cached.putObject("receipts");
        cached.put("lastCommit", checkpoint.path("lastCommit").asText(null));
        // Anchor the replay at the latest quiet checkpoint instead of the Durable position: the
        // position may sit inside an open step, whose start the suffix replay would then miss.
        // Without a quiet point, replay fully.
        long durPos = checkpoint.isNull() ? 0 : checkpoint.path("position").asLong(0);
        JournalEvent anchor = quietCheckpoint(durPos);
        baseSeq = anchor == null ? 0 : anchor.seq();
        long position = baseSeq;
        long lastCheckpoint = baseSeq;
        while (true) {
            var page = context.journal(position, 1000);
            for (var entry : page) {
                var event = DurableJson.convert(entry, JournalEvent.class);
                JournalEventTypes.validate(event);
                if (event.seq() != ++position)
                    throw new DurableException(
                            DurableException.Code.CONFLICT, "Native journal is discontinuous");
                if ("state/checkpoint".equals(event.type())) lastCheckpoint = event.seq();
                cached.withArray("events").add(entry);
                messages.accept(event);
                // Mirror the committed prefix into the presentation state machine: after a
                // restart the first live delta must still resolve its Step/message mapping.
                liveMessages.accept(event);
                toolEvents.accept(event, messages);
            }
            if (page.size() < 1000) break;
        }
        checkpointGap = position - lastCheckpoint;
        if (checkpoint.path("position").asLong() != position)
            throw new DurableException(
                    DurableException.Code.CONFLICT, "Native checkpoint and journal disagree");
    }

    /**
     * Queues result metadata for the recorder's next commit. It must not append its own batch:
     * that would advance the journal head past the recorder's preallocated sequence and fence the
     * next native commit with a conflict.
     */
    synchronized void queueResultMetadata(String runId, Map<String, Object> values) {
        if (runId == null || values == null || values.isEmpty()) return;
        var merged = new LinkedHashMap<String, Object>();
        var previous = queuedResultMetadata.get(runId);
        if (previous != null) merged.putAll(previous.values());
        merged.putAll(values);
        queuedResultMetadata.put(runId, new QueuedResultMetadata(runId, merged));
    }

    /**
     * Publishes metadata that arrived after the recorder's final commit. The flush writes one
     * marker fact so the existing commit path stays the only writer; the payload itself rides on
     * the public event.
     */
    synchronized void flushQueuedResultMetadata(String turnId) {
        if (turnId == null || queuedResultMetadata.isEmpty()) return;
        try {
            append(turnId, List.of(Map.entry(DurableEventSchemas.RESULT_METADATA, Map.of())));
        } catch (RuntimeException ignored) {
            // Optional metadata must not turn a completed invocation into a failure.
        }
    }

    void modelStarted(String id, ModelCapture capture) {
        captures.put(id, capture);
    }

    /**
     * Must not take the journal monitor: it runs on the model stream thread, and a commit holds
     * that monitor for its database round trips, which would stall reading the next chunk.
     */
    void modelChunk(String id, ChatResponse chunk) {
        captures.get(id).accept(chunk);
    }

    private JournalEvent capture(JournalEvent event) {
        if (!"model/end".equals(event.type())) return event;
        var capture = captures.get(String.valueOf(event.data().get("modelCallId")));
        if (capture == null) return event;
        var data = new LinkedHashMap<String, Object>(event.data());
        data.put(ModelCapture.OUTCOME, capture.freeze(String.valueOf(data.get("status"))));
        return new JournalEvent(
                event.schemaVersion(),
                event.eventId(),
                event.seq(),
                event.occurredAt(),
                event.type(),
                event.executionRunId(),
                event.turnId(),
                event.required(),
                JsonUtils.getJsonCodec().toJson(data));
    }

    synchronized void consuming(List<String> signals) {
        signalsToApply.addAll(signals);
    }

    synchronized boolean hasUnsettledNativeTools(String turnId) {
        return toolEvents.hasUnsettledTools(turnId);
    }

    synchronized boolean unsettledNativeTool(String turnId, String callId) {
        return toolEvents.unsettled(turnId, callId);
    }

    private ObjectNode checkpoint() {
        return cached.deepCopy();
    }

    /**
     * Forwards a streaming delta as soon as it is appended instead of waiting for the next batched
     * commit. The attempt boundary is a stream-time fact: the adapter accumulates chunk usage as
     * the model streams, so the reset must run when request_started is appended. Resetting when
     * the batch commits would wipe chunks that streamed before a commit that batches
     * request_started with its request_ended (append-merged attempts). Committed events never
     * produce live output on the commit path, so each event is published exactly once.
     */
    @Override
    public void live(JournalEvent event) {
        if ("model/request_started".equals(event.type())) {
            var capture = captures.get(String.valueOf(event.data().get("modelCallId")));
            if (capture != null) capture.nextAttempt();
        }
        AgentScopeMessageEvents.Output output;
        synchronized (liveLock) {
            try {
                output = liveMessages.accept(event);
            } catch (RuntimeException ignored) {
                // A malformed preview must not break recording; the committed path reports it.
                return;
            }
        }
        if (!output.live().isEmpty()) {
            context.publish(output.live());
        }
    }

    @Override
    public synchronized long exportCursor(String sink) {
        return exportCursors.getOrDefault(sink, 0L);
    }

    @Override
    public synchronized void advanceExportCursor(String sink, long expected, long next) {
        if (exportCursor(sink) != expected)
            throw new DurableException(
                    DurableException.Code.CONFLICT, "Export cursor moved for sink " + sink);
        exportCursors.put(sink, next);
    }

    @Override
    public synchronized Head head() {
        // head(...) only reads the state; copying the whole event array per call dominated
        // recovery and every append on a long thread.
        return head(cached);
    }

    private Head head(ObjectNode state) {
        return new Head(
                FORMAT_VERSION,
                baseSeq + state.path("events").size(),
                context.lease().epoch(),
                owner,
                context.lease().expiresAt(),
                state.path("lastCommit").asText(null));
    }

    @Override
    public synchronized Writer acquire(String requested, Duration lease) {
        context.frame();
        if (sealed.contains(requested) || (owner != null && !owner.equals(requested)))
            throw new DurableException(
                    DurableException.Code.FENCED, "Native writer is unavailable");
        owner = requested;
        return new Writer(owner, context.lease().epoch());
    }

    @Override
    public synchronized Head sealWriter(String requested) {
        sealed.add(requested);
        if (requested.equals(owner)) owner = null;
        return head();
    }

    private void check(Writer writer) {
        // Ownership is enforced atomically by commit/owned(); an extra Frame read here would be
        // a write transaction before every flush and could not prove the lease at commit time.
        if (!writer.owner().equals(owner)
                || writer.epoch() != context.lease().epoch()
                || sealed.contains(writer.owner()))
            throw new DurableException(DurableException.Code.FENCED, "Stale native writer");
    }

    @Override
    public synchronized void renew(Writer writer, Duration lease) {
        // Only the runtime owns lease renewal; this guard only proves the writer still belongs to
        // this host. Reading a Frame here would add a write transaction to every flush batch and
        // compete with commit for the session row lock.
        if (!writer.owner().equals(owner)
                || writer.epoch() != context.lease().epoch()
                || sealed.contains(writer.owner()))
            throw new DurableException(DurableException.Code.FENCED, "Stale native writer");
    }

    @Override
    public synchronized void release(Writer writer) {
        if (writer.owner().equals(owner)) owner = null;
    }

    @Override
    public synchronized Head commit(
            Writer writer, String batchId, long expected, List<JournalEvent> events) {
        return commit(writer, batchId, expected, events, TurnStatus.RUNNING, null, true);
    }

    private Head commit(
            Writer writer,
            String batchId,
            long expected,
            List<JournalEvent> events,
            TurnStatus turnStatus,
            String terminalError,
            boolean invocationFacts) {
        return commit(
                writer,
                batchId,
                expected,
                events,
                turnStatus,
                terminalError,
                invocationFacts,
                List.of(),
                null);
    }

    private Head commit(
            Writer writer,
            String batchId,
            long expected,
            List<JournalEvent> events,
            TurnStatus turnStatus,
            String terminalError,
            boolean invocationFacts,
            List<EventInput> extraPublic,
            WaitCondition waiting) {
        events = events.stream().map(this::capture).toList();
        var state = checkpoint();
        String digest =
                DurableJson.hash(
                        Map.of(
                                "expected",
                                expected,
                                "events",
                                events,
                                "status",
                                turnStatus,
                                "error",
                                DurableJson.value(terminalError)));
        var previous = state.path("receipts").get(batchId);
        if (previous != null) {
            if (!digest.equals(previous.path("digest").asText()))
                throw new DurableException(
                        DurableException.Code.CONFLICT, "Native batch content changed");
            return DurableJson.convert(previous.path("head"), Head.class);
        }
        check(writer);
        if (baseSeq + state.path("events").size() != expected)
            throw new DurableException(DurableException.Code.CONFLICT, "Native sequence changed");
        var tools = new ArrayList<ToolCall>();
        var publicEvents = new ArrayList<EventInput>();
        var liveEvents = new ArrayList<LiveEventInput>();
        // Result metadata precedes every event this commit derives, so a client sees it before
        // run.ended closes the Run. The event id stays stable across retries of the same value.
        for (var metadata : queuedResultMetadata.values())
            publicEvents.add(
                    new EventInput(
                            "x.result_metadata:"
                                    + metadata.runId()
                                    + ":"
                                    + DurableJson.hash(metadata.values()),
                            "x.result_metadata",
                            DurableJson.value(metadata.values()),
                            metadata.runId(),
                            null));
        var nextMessages = new AgentScopeMessageEvents(messages);
        var nextToolEvents = new AgentScopeToolEvents(toolEvents);
        RunChange runChange = null;
        var stepChanges = new ArrayList<StepChange>();
        var appliedCalls = new LinkedHashMap<String, RunStepRef>();
        boolean hasCheckpoint = false;
        long seq = expected;
        for (var event : events) {
            int firstPublicEvent = publicEvents.size();
            JournalEventTypes.validate(event);
            if (event.seq() != ++seq)
                throw new DurableException(
                        DurableException.Code.CONFLICT, "Non-contiguous native batch");
            state.withArray("events").add(DurableJson.value(event));
            var data = event.data();
            if ("verification/result".equals(event.type()) && data.get("verification") != null) {
                var check =
                        JsonUtils.getJsonCodec()
                                .convertValue(data.get("verification"), TaskVerification.class);
                var evidence = check.binding();
                publicEvents.add(
                        new EventInput(
                                event.eventId(),
                                VerificationView.EVENT_TYPE,
                                DurableJson.value(
                                        new VerificationView(
                                                check.id(),
                                                check.requirementId(),
                                                check.actionId(),
                                                check.verifierId(),
                                                check.outcome().name(),
                                                evidence.taskId(),
                                                evidence.contractVersion(),
                                                evidence.subjectVersion(),
                                                check.recordedAt()))));
            }
            if ("input/applied".equals(event.type()) && initialInputId.equals(data.get("inputId")))
                publicEvents.add(initialInputApplication());
            if ("run/start".equals(event.type())) {
                runChange = new RunChange(event.executionRunId(), RunStatus.RUNNING, null, null);
            } else if ("run/end".equals(event.type())) {
                var outcome = JsonUtils.getJsonCodec().convertValue(data, RunOutcome.class);
                RunStatus status =
                        switch (outcome.status()) {
                            case COMPLETED -> RunStatus.COMPLETED;
                            case FAILED -> RunStatus.FAILED;
                            case SUSPENDED, INTERRUPTED, CANCELLED -> RunStatus.STOPPED;
                        };
                runChange =
                        new RunChange(
                                event.executionRunId(),
                                status,
                                outcome.status().value(),
                                outcome.reason());
            } else if (Set.of("step/start", "step/phase", "step/end").contains(event.type())) {
                String outcome = (String) data.get("status");
                StepStatus status =
                        !"step/end".equals(event.type())
                                ? StepStatus.RUNNING
                                : Set.of("continued", "completed").contains(outcome)
                                        ? StepStatus.COMPLETED
                                        : "failed".equals(outcome)
                                                ? StepStatus.FAILED
                                                : StepStatus.STOPPED;
                stepChanges.add(
                        new StepChange(
                                event.executionRunId(),
                                (String) data.get("stepId"),
                                status,
                                StepPhase.valueOf((String) data.get("phase")),
                                (String) data.get("resumedFromStepId"),
                                outcome));
            }
            if ("interaction/requested".equals(event.type())
                    && "external_execution".equals(data.get("kind"))) {
                var call =
                        JsonUtils.getJsonCodec().convertValue(data.get("call"), ToolUseBlock.class);
                String executor = executors.get(call.getName());
                if (executor != null)
                    tools.add(
                            new ToolCall(
                                    call.getId(),
                                    executor,
                                    call.getName(),
                                    DurableJson.value(call.getInput()),
                                    bindings.containsKey(call.getName())
                                            ? bindings.get(call.getName()).digest()
                                            : null,
                                    new RunStepRef(
                                            event.executionRunId(), (String) data.get("stepId"))));
            }
            if ("interaction/requested".equals(event.type())
                    || "interaction/resolved".equals(event.type())) {
                String requestId = String.valueOf(data.get("requestId"));
                var view = new LinkedHashMap<String, Object>();
                view.put("id", DurableJson.hash(List.of(context.lease().turnId(), requestId)));
                view.put("requestId", requestId);
                view.put("turnId", context.lease().turnId());
                view.put("key", "interaction:" + requestId);
                view.put("kind", String.valueOf(data.get("kind")));
                boolean requested = "interaction/requested".equals(event.type());
                view.put(
                        "status",
                        requested
                                ? "pending"
                                : "cancelled".equals(data.get("status"))
                                        ? "cancelled"
                                        : "resolved");
                if (requested) {
                    var call =
                            JsonUtils.getJsonCodec()
                                    .convertValue(data.get("call"), ToolUseBlock.class);
                    view.put("call", Map.of("name", call.getName(), "arguments", call.getInput()));
                    view.put(
                            "automatic",
                            "external_execution".equals(data.get("kind"))
                                    && executors.containsKey(call.getName()));
                }
                publicEvents.add(
                        new EventInput(
                                event.eventId(),
                                requested ? "interaction.requested" : "interaction.resolved",
                                DurableJson.value(view)));
            }
            if ("state/checkpoint".equals(event.type())) {
                hasCheckpoint = true;
                var nativeState = AgentState.fromJsonString((String) data.get("stateJson"));
                for (var message : nativeState.getContext())
                    for (var result : message.getContentBlocks(ToolResultBlock.class))
                        if (!result.isSuspended())
                            appliedCalls.putIfAbsent(
                                    result.getId(),
                                    invocationFacts
                                            ? new RunStepRef(
                                                    event.executionRunId(),
                                                    (String) data.get("stepId"))
                                            : null);
            }
            var message = nextMessages.accept(event);
            if (message.durable() != null) publicEvents.add(message.durable());
            // Resolve the Step message id against the machine that just accepted this event:
            // a stopped partial must reconcile with the Step's live preview, and its request
            // may share this very batch.
            var modelEvent = modelEvent(event, nextMessages);
            if (modelEvent != null) {
                publicEvents.add(firstPublicEvent, modelEvent);
            }
            // Live previews are published when events are appended through the live mirror,
            // never here: the commit path would otherwise deliver every delta a second time.
            publicEvents.addAll(nextToolEvents.accept(event, nextMessages));
            var toolPreview = nextToolEvents.preview(event, context.lease().epoch());
            if (toolPreview != null) liveEvents.add(toolPreview);
            if (data.get("stepId") instanceof String stepId) {
                for (int i = firstPublicEvent; i < publicEvents.size(); i++) {
                    var output = publicEvents.get(i);
                    publicEvents.set(
                            i,
                            new EventInput(
                                    output.eventId(),
                                    output.type(),
                                    output.data(),
                                    event.executionRunId(),
                                    stepId));
                }
            }
        }
        state.put("lastCommit", batchId);
        Head result = head(state);
        state.withObject("/receipts")
                .set(batchId, DurableJson.value(Map.of("digest", digest, "head", result)));
        // Caller-supplied public events (for example the UNKNOWN tool updates of a parking
        // commit) ride the same Backend transaction as the journal facts above.
        publicEvents.addAll(extraPublic);
        var frame = context.frame();
        var applied =
                frame.tools().stream()
                        .filter(
                                t ->
                                        !t.applied()
                                                && t.result().state().terminal()
                                                && appliedCalls.containsKey(
                                                        t.request().call().callId()))
                        .map(
                                t ->
                                        new AppliedResult(
                                                t.request().executionId(),
                                                appliedCalls.get(t.request().call().callId())))
                        .toList();
        var consumed = hasCheckpoint ? List.copyOf(signalsToApply) : List.<String>of();
        commitNative(
                new Commit(
                        "native:" + batchId,
                        frame.turn().revision(),
                        DurableJson.value(
                                Map.of(
                                        "format",
                                        AgentScopeCheckpointCodec.STATE_VERSION,
                                        "position",
                                        seq,
                                        "lastCommit",
                                        batchId)),
                        tools,
                        applied,
                        consumed,
                        publicEvents,
                        turnStatus,
                        waiting,
                        null,
                        terminalError,
                        new JournalAppend(
                                expected, events.stream().map(DurableJson::value).toList()),
                        runChange,
                        stepChanges));
        context.publish(liveEvents);
        queuedResultMetadata.clear();
        cached = state;
        messages = nextMessages;
        toolEvents = nextToolEvents;
        if (hasCheckpoint) signalsToApply.clear();
        return result;
    }

    private EventInput initialInputApplication() {
        var input =
                initialInput.isTextual()
                        ? Input.text(initialInput.asText())
                        : DurableJson.convert(initialInput, Input.class);
        return new EventInput(
                "input-applied:" + initialInputId,
                "input.applied",
                DurableJson.value(new Message("1", initialInputId, "user", input.content())));
    }

    private static TokenUsage usage(JsonNode value) {
        if (value == null || value.isNull()) return null;
        var details = new LinkedHashMap<String, Long>();
        // Core's optional primitive breakdowns use zero for unreported values; do not claim
        // these are known zeros. Main input/output totals are defined when ChatUsage is present.
        for (String name :
                List.of(
                        "cachedTokens",
                        "cacheCreationTokens",
                        "reasoningTokens",
                        "toolUsePromptTokens"))
            if (value.path(name).asLong() > 0) details.put(name, value.path(name).asLong());
        return new TokenUsage(
                value.required("inputTokens").asLong(),
                value.required("outputTokens").asLong(),
                details);
    }

    private static EventInput modelEvent(JournalEvent event, AgentScopeMessageEvents committed) {
        String type =
                switch (event.type()) {
                    case "request/prepared" -> "model.call.started";
                    case "model/end" -> "model.call.ended";
                    case "model/request_started" -> "model.request.started";
                    case "model/request_usage" -> "model.request.usage";
                    case "model/request_ended" -> "model.request.ended";
                    default -> null;
                };
        if (type == null) return null;
        var nativeData = DurableJson.value(event.data());
        var data = new LinkedHashMap<String, Object>();
        data.put("modelCallId", nativeData.required("modelCallId").asText());
        if (type.equals("model.call.started")) {
            data.put("model", nativeData.required("model").asText());
            data.put("purpose", nativeData.required("purpose").asText());
            data.put("iteration", nativeData.required("iteration").asInt());
            data.put("startedAt", event.occurredAt());
        } else if (type.equals("model.call.ended")) {
            data.put("status", nativeData.required("status").asText().toUpperCase(Locale.ROOT));
            data.put("endedAt", event.occurredAt());
            data.put("usage", usage(nativeData.get("usage")));
            // A stopped model call carries the assembled partial as one committed fact so a
            // reconnect can render it without replaying per-token deltas. The Step message id
            // lets the client reconcile it with the live preview it may already have shown.
            var outcome = nativeData.path(ModelCapture.OUTCOME);
            String partial = outcome.path("partial").path("text").asText(null);
            if (partial != null && !partial.isEmpty()) {
                String callId = nativeData.path("modelCallId").asText(null);
                String messageId = callId == null ? null : committed.stepMessageId(callId);
                var stopped = new LinkedHashMap<String, Object>();
                stopped.put("text", partial);
                stopped.put("interrupted", true);
                data.put("partial", stopped);
                // Top-level like message.completed so preview tracking reconciles by Step.
                if (messageId != null) data.put("messageId", messageId);
            }
        } else {
            data.put("requestId", nativeData.required("requestId").asText());
            if (type.equals("model.request.started")) {
                data.put("provider", nativeData.required("provider").asText());
                data.put("attempt", nativeData.required("attempt").asInt());
                data.put("startedAt", nativeData.required("startedAt").asLong());
            } else {
                data.put("usage", usage(nativeData.get("usage")));
                if (type.equals("model.request.ended")) {
                    data.put(
                            "status",
                            nativeData.required("status").asText().toUpperCase(Locale.ROOT));
                    data.put("endedAt", nativeData.required("endedAt").asLong());
                    data.put("durationMillis", nativeData.required("durationMillis").asLong());
                    data.put("usageComplete", nativeData.required("usageComplete").asBoolean());
                    data.put("errorType", nativeData.path("errorType").asText(null));
                }
            }
        }
        return new EventInput(
                event.eventId(),
                type,
                DurableJson.value(data),
                event.executionRunId(),
                nativeData.path("stepId").asText(null));
    }

    private void commitNative(Commit command) {
        try {
            context.commit(command);
        } catch (DurableException conflict) {
            if (conflict.code() != DurableException.Code.CONFLICT) throw conflict;
            var current = context.frame();
            if (current.turn().status() != TurnStatus.PAUSING
                    || current.turn().revision() == command.expectedRevision()) throw conflict;
            // A pause changes the control revision without invalidating this native checkpoint.
            // The journal position, ownership, inputs and effects are still checked atomically.
            context.commit(
                    new Commit(
                            command.commitId(),
                            current.turn().revision(),
                            command.checkpoint(),
                            command.tools(),
                            command.appliedResults(),
                            command.consumedSignals(),
                            command.events(),
                            command.status(),
                            command.waiting(),
                            command.output(),
                            command.error(),
                            command.journal(),
                            command.run(),
                            command.steps()));
        }
    }

    /** Committed sequence this invocation restored from; history before it is not in memory. */
    long baseSeq() {
        return baseSeq;
    }

    @Override
    public long replayStart() {
        return baseSeq;
    }

    @Override
    public String threadId() {
        return context.lease().thread().threadId();
    }

    private record WindowScan(JournalEvent anchor, List<JournalEvent> entries, long from) {}

    /**
     * Fetches the replay window ending at {@code upper} and finds the latest quiet checkpoint in
     * it. Pages backward (up to four extra pages) until a full checkpoint is covered, so the
     * seed can account for every fact the anchor state does not fold. Returns a null anchor when
     * no quiet point proves the required coverage.
     */
    private WindowScan scanWindow(long upper) {
        if (upper == 0) return new WindowScan(null, List.of(), 0);
        long from = Math.max(0, upper - 500);
        var entries = new ArrayList<JournalEvent>();
        for (var entry : context.journal(from, (int) (upper - from)))
            entries.add(DurableJson.convert(entry, JournalEvent.class));
        for (int page = 0; page < 4 && from > 0 && !hasFullCheckpoint(entries); page++) {
            long next = Math.max(0, from - 1000);
            var prefix = new ArrayList<JournalEvent>();
            for (var entry : context.journal(next, (int) (from - next)))
                prefix.add(DurableJson.convert(entry, JournalEvent.class));
            prefix.addAll(entries);
            entries = prefix;
            from = next;
        }
        int start = 0;
        if (from > 0) {
            // A window that starts mid-step cannot prove there is no earlier open step/tool.
            // Resume from the latest step-boundary checkpoint inside the window, or replay fully.
            start = -1;
            for (int i = 0; i < entries.size(); i++) {
                var candidate = entries.get(i);
                if ("state/checkpoint".equals(candidate.type())
                        && "step_boundary".equals(candidate.data().get("reason"))) start = i + 1;
            }
            if (start < 0) return new WindowScan(null, entries, from);
        }
        var openSteps = new java.util.LinkedHashSet<String>();
        var openTools = new java.util.LinkedHashSet<String>();
        JournalEvent quiet = null;
        // Open/close tracking runs from the window start, but only checkpoints at or after the
        // step boundary can anchor: a close observed post-start proves nothing about a request
        // that predates the window, while an open that predates it must block every later
        // checkpoint. Tracking from 0 (instead of start) keeps pre-boundary opens visible, so a
        // checkpoint after their resolution is still judged non-quiet.
        for (int i = 0; i < entries.size(); i++) {
            var event = entries.get(i);
            var data = event.data();
            switch (event.type()) {
                case "step/start" -> {
                    if (data.get("stepId") instanceof String id) openSteps.add(id);
                }
                case "step/end" -> {
                    if (data.get("stepId") instanceof String id
                            && Set.of("completed", "continued").contains(data.get("status")))
                        openSteps.remove(id);
                }
                case "tool/requested" -> {
                    if (data.get("call") instanceof Map<?, ?> call
                            && call.get("id") instanceof String id) openTools.add(id);
                }
                case "interaction/requested" -> {
                    if (data.get("requestId") != null)
                        openTools.add(String.valueOf(data.get("requestId")));
                }
                case "action/end" -> {
                    if (data.get("toolCallId") instanceof String id) openTools.remove(id);
                }
                case "tool/result" -> {
                    // State application closes what action/end left open: synthetic settlement
                    // results never pass through action/end, so without this their callIds
                    // would pin openTools forever and no checkpoint could ever anchor again.
                    var message = data.get("message");
                    if (message != null) {
                        try {
                            var resultMessage =
                                    JsonUtils.getJsonCodec().convertValue(message, Msg.class);
                            for (var result : resultMessage.getContentBlocks(ToolResultBlock.class))
                                openTools.remove(result.getId());
                        } catch (RuntimeException ignored) {
                            // Conservative: an unreadable result must not fake a quiet anchor.
                        }
                    }
                }
                case "interaction/resolved" -> {
                    if (data.get("requestId") != null)
                        openTools.remove(String.valueOf(data.get("requestId")));
                }
                case "state/checkpoint" -> {
                    if (i >= start && openSteps.isEmpty() && openTools.isEmpty()) quiet = event;
                }
                default -> {}
            }
        }
        return new WindowScan(quiet, entries, from);
    }

    private static boolean hasFullCheckpoint(List<JournalEvent> entries) {
        for (var event : entries) if (isFullCheckpoint(event)) return true;
        return false;
    }

    private static boolean isFullCheckpoint(JournalEvent event) {
        return "state/checkpoint".equals(event.type())
                && !"step_boundary".equals(event.data().get("reason"))
                && !"periodic".equals(event.data().get("reason"));
    }

    private JournalEvent quietCheckpoint(long upper) {
        return scanWindow(upper).anchor();
    }

    @Override
    public ProjectionSeed projectionSeed(long fromSeq) {
        if (projectionCache != null && projectionCacheFrom == fromSeq) return projectionCache;
        long upper = Math.min(fromSeq, head().seq());
        if (upper == 0) return null;
        var scan = scanWindow(upper);
        JournalEvent anchor = scan.anchor();
        if (anchor == null) return null;
        var entries = scan.entries();
        // Facts the anchor state does not fold must be replayed from the latest full checkpoint.
        int full = -1;
        for (int i = 0; i < entries.size(); i++) {
            var event = entries.get(i);
            if (event.seq() <= anchor.seq() && isFullCheckpoint(event)) full = i;
        }
        if (full < 0) return null;
        var data = anchor.data();
        Object stateJson = data.get("stateJson");
        if (stateJson == null) return null;
        // The checkpoint event itself carries the run that wrote it. Scanning must stop at the
        // checkpoint: a run/start after it belongs to a later run, not to this snapshot.
        String positionRunId = anchor.executionRunId();
        if (positionRunId == null) {
            for (int i = entries.size() - 1; i >= 0 && entries.get(i).seq() < anchor.seq(); i--) {
                var event = entries.get(i);
                if ("run/start".equals(event.type())) {
                    positionRunId = event.executionRunId();
                    break;
                }
            }
        }
        // Without a run identity the seeded view cannot answer positionRunId(); that is the case
        // for pause/fork paths whose run is only known from Durable state, so replay fully.
        if (positionRunId == null) return null;
        var uncertain = new java.util.LinkedHashSet<String>();
        var activeRuns = new java.util.LinkedHashSet<String>();
        for (var tool : context.frame().tools()) {
            if (!tool.result().state().terminal()) {
                uncertain.add(tool.request().call().callId());
                var origin = tool.request().call().origin();
                if (origin != null) activeRuns.add(origin.runId());
            }
        }
        activeRuns.add(positionRunId);
        var results = new LinkedHashMap<String, ToolResultBlock>();
        var verifications = new ArrayList<TaskVerification>();
        var extensions = new ArrayList<JournalEvent>();
        var messageIds = new java.util.LinkedHashSet<String>();
        var appliedInputs = new java.util.LinkedHashSet<String>();
        String latestRun = null;
        String latestTurn = null;
        boolean finished = false;
        String interruptedStep = null;
        for (int i = full + 1; i < entries.size() && entries.get(i).seq() <= anchor.seq(); i++) {
            var event = entries.get(i);
            var facts = event.data();
            switch (event.type()) {
                case "state/checkpoint",
                        "state/initialized",
                        "migration/baseline",
                        "state/restored" -> {
                    verifications.clear();
                    extensions.clear();
                    if (Set.of("state/initialized", "state/restored").contains(event.type())) {
                        latestRun = null;
                        latestTurn = null;
                        interruptedStep = null;
                        appliedInputs.clear();
                        finished = true;
                    }
                    messageIds.addAll(historyMessageIds(event));
                }
                case "verification/result" -> {
                    if (facts.get("verification") != null)
                        verifications.add(
                                JsonUtils.getJsonCodec()
                                        .convertValue(
                                                facts.get("verification"), TaskVerification.class));
                    messageIds.addAll(historyMessageIds(event));
                }
                case "tool/dispatch" -> {
                    uncertain.add((String) facts.get("toolCallId"));
                    messageIds.addAll(historyMessageIds(event));
                }
                case "action/end" -> {
                    if (facts.get("result") != null) {
                        String id = (String) facts.get("toolCallId");
                        var result =
                                JsonUtils.getJsonCodec()
                                        .convertValue(facts.get("result"), ToolResultBlock.class);
                        if (result.hasUnknownOutcome()
                                || uncertain.contains(id)
                                        && result.getState() == ToolResultState.INTERRUPTED) {
                            uncertain.add(id);
                            results.remove(id);
                        } else {
                            uncertain.remove(id);
                            results.put(id, result);
                        }
                    }
                    messageIds.addAll(historyMessageIds(event));
                }
                case "run/start" -> {
                    latestRun = event.executionRunId();
                    latestTurn = event.turnId();
                    finished = false;
                    activeRuns.add(event.executionRunId());
                    messageIds.addAll(historyMessageIds(event));
                }
                case "run/end" -> {
                    activeRuns.remove(event.executionRunId());
                    if (Objects.equals(latestRun, event.executionRunId()))
                        finished =
                                Set.of("completed", "cancelled").contains(facts.get("status"))
                                        || "MAX_ITERATIONS".equals(facts.get("reason"))
                                        || "ALL_TOOLS_DENIED".equals(facts.get("reason"));
                    messageIds.addAll(historyMessageIds(event));
                }
                case "input/applied" -> {
                    if (facts.get("inputId") != null)
                        appliedInputs.add(String.valueOf(facts.get("inputId")));
                    messageIds.addAll(historyMessageIds(event));
                }
                case "step/start" -> {
                    interruptedStep = (String) facts.get("stepId");
                    messageIds.addAll(historyMessageIds(event));
                }
                case "step/end" -> {
                    if (Set.of("completed", "continued").contains(facts.get("status")))
                        interruptedStep = null;
                    messageIds.addAll(historyMessageIds(event));
                }
                case "recovery/applied" -> {
                    Object interrupted = facts.get("interruptedRuns");
                    if (interrupted instanceof List<?> ids) ids.forEach(activeRuns::remove);
                    Object resolved = facts.get("resolvedToolCallIds");
                    if (resolved instanceof List<?> ids) ids.forEach(uncertain::remove);
                    messageIds.addAll(historyMessageIds(event));
                }
                default -> {
                    if (!JournalEventTypes.BUILTIN.contains(event.type())) extensions.add(event);
                    messageIds.addAll(historyMessageIds(event));
                }
            }
        }
        // The anchor state already folds every message recorded at or before it.
        try {
            var restored = AgentState.fromJsonString(String.valueOf(stateJson));
            for (var message : restored.getContext()) messageIds.add(message.getId());
        } catch (RuntimeException ignored) {
            // A state that cannot be restored cannot seed recovery either.
            return null;
        }
        io.agentscope.core.agent.AgentExecutionPosition position =
                data.get("executionPosition") == null
                        ? null
                        : JsonUtils.getJsonCodec()
                                .convertValue(
                                        data.get("executionPosition"),
                                        io.agentscope.core.agent.AgentExecutionPosition.class);
        projectionCacheFrom = fromSeq;
        projectionCache =
                new ProjectionSeed(
                        anchor.seq(),
                        String.valueOf(stateJson),
                        uncertain,
                        activeRuns,
                        anchor.turnId(),
                        positionRunId,
                        position,
                        data.get("checkpointId") instanceof String id ? id : null,
                        results,
                        verifications,
                        extensions,
                        messageIds,
                        appliedInputs,
                        latestRun,
                        latestTurn,
                        finished,
                        interruptedStep,
                        data.get("pendingModelCallId") instanceof String id ? id : null,
                        data.get("modelAnswerId") instanceof String id ? id : null,
                        anchor.turnId());
        return projectionCache;
    }

    /**
     * Message identities one journal event carries, mirroring the recorder's history view.
     */
    private List<String> historyMessageIds(JournalEvent event) {
        return historyMessages(event).stream().map(Msg::getId).toList();
    }

    private List<Msg> historyMessages(JournalEvent event) {
        if (Set.of("state/initialized", "migration/baseline", "state/restored", "state/checkpoint")
                .contains(event.type())) {
            String state = (String) materialize(event).data().get("stateJson");
            return state == null ? List.of() : AgentState.fromJsonString(state).getContext();
        }
        if (Set.of("message/user", "message/assistant", "tool/result", "run/output")
                .contains(event.type())) {
            Object value = event.data().get("message");
            if (value != null)
                return List.of(JsonUtils.getJsonCodec().convertValue(value, Msg.class));
        }
        return List.of();
    }

    @Override
    public long checkpointGap() {
        return checkpointGap;
    }

    synchronized String messageId(String turnId, Msg message) {
        return messages.messageId(turnId, message);
    }

    @Override
    public synchronized List<JournalEvent> readAfter(long after, int limit) {
        if (after < 0 || limit < 1) throw new IllegalArgumentException("Invalid native range");
        var result = new ArrayList<JournalEvent>();
        // History at or before baseSeq lives only in the store; the cached array starts after it.
        if (after < baseSeq) {
            for (var entry : context.journal(after, (int) Math.min(limit, baseSeq - after))) {
                result.add(DurableJson.convert(entry, JournalEvent.class));
                if (result.size() >= limit) return List.copyOf(result);
            }
        }
        var events = cached.path("events");
        for (long i = Math.max(after, baseSeq);
                i < baseSeq + events.size() && result.size() < limit;
                i++)
            result.add(DurableJson.convert(events.get((int) (i - baseSeq)), JournalEvent.class));
        return List.copyOf(result);
    }

    void append(String turnId, List<Map.Entry<String, Object>> facts) {
        append(turnId, facts, TurnStatus.RUNNING, null);
    }

    /** Native settlement and the terminal task outcome share one Backend transaction. */
    synchronized void append(
            String turnId, List<Map.Entry<String, Object>> facts, TurnStatus status, String error) {
        append(turnId, facts, status, error, null);
    }

    /**
     * Settlement variant that attributes message-like facts to the interrupted native Run.
     * A null {@code executionRunId} keeps the historical writer-owner marker; message facts
     * need the real Run so their public events rejoin the interrupted Run instead of naming
     * a Run that was never created.
     */
    synchronized void append(
            String turnId,
            List<Map.Entry<String, Object>> facts,
            TurnStatus status,
            String error,
            String executionRunId) {
        appendWaiting(turnId, facts, status, error, List.of(), null, executionRunId);
    }

    /**
     * Journal facts plus caller-supplied public events plus the waiting condition share one
     * Backend transaction. Parking a native-tool wait must not split the marker from the
     * WAITING status: a journal-only commit would clear the wait, and a status-only commit
     * would lose the attempt count on crash.
     */
    synchronized void appendWaiting(
            String turnId,
            List<Map.Entry<String, Object>> facts,
            TurnStatus status,
            String error,
            List<EventInput> extraPublic,
            WaitCondition waiting,
            String executionRunId) {
        var writer = acquire("adapter:" + UUID.randomUUID(), Duration.ofMinutes(1));
        try {
            long sequence = head().seq();
            var events = new ArrayList<JournalEvent>();
            for (var fact : facts)
                events.add(
                        new JournalEvent(
                                JournalEvent.SCHEMA_VERSION,
                                UUID.randomUUID().toString(),
                                ++sequence,
                                System.currentTimeMillis(),
                                fact.getKey(),
                                executionRunId != null ? executionRunId : writer.owner(),
                                turnId,
                                true,
                                JsonUtils.getJsonCodec().toJson(fact.getValue())));
            commit(
                    writer,
                    UUID.randomUUID().toString(),
                    sequence - events.size(),
                    events,
                    status,
                    error,
                    false,
                    extraPublic,
                    waiting);
        } finally {
            release(writer);
        }
    }
}
