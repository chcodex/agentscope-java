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
package io.agentscope.durable.agui;

import com.fasterxml.jackson.databind.JsonNode;
import io.agentscope.core.agui.AguiInterruptConstants;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.durable.DurableJson;
import io.agentscope.durable.DurableTypes.LiveEvent;
import io.agentscope.durable.DurableTypes.PublicEvent;
import io.agentscope.durable.DurableTypes.ToolOutputDelta;
import io.agentscope.durable.EventStreamOptions.EventDeltaType;
import io.agentscope.durable.SessionStreamEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Stateful Durable-to-AG-UI 1.0 mapping. One adapter belongs to one stream and reconciles live
 * message previews with their authoritative committed message.
 *
 * <p>A Run that parks for human input reports {@code run.ended} with {@code STOPPED} before the
 * committed {@code turn.waiting} event, so that end is not terminal. Only {@code turn.waiting}
 * closes the stream and carries the pending interactions: it maps to one {@link
 * AguiEvent.RunFinished} with a {@link AguiEvent.RunFinishedInterruptOutcome}, after closing the
 * steps, messages and tool calls the Run opened. Individual {@code interaction.requested} events
 * are not terminal and are not mapped on their own. Each interrupt id is the Durable interaction
 * id, and its {@code toolCallId} is the interaction request id (the tool call id), which the client
 * echoes as {@code interruptId} to resume.
 *
 * <p>A waiting Turn without pending interactions (for example waiting on a signal) has no AG-UI
 * interrupt to deliver; its stream follows the Run's own terminal event, if any.
 */
public final class DurableAguiEventAdapter {
    /** Run reasons that park the Run for human input instead of ending it. */
    private static final Set<String> PARKED_RUN_REASONS =
            Set.of("suspended", "PERMISSION_ASKING", "TOOL_SUSPENDED");

    private static final Map<String, Object> CONFIRM_RESPONSE_SCHEMA = Map.of("type", "boolean");

    private final Set<String> messages = new HashSet<>();
    private final Map<String, StringBuilder> messageText = new HashMap<>();
    private final Set<String> reasoningMessages = new HashSet<>();
    private String openReasoning;
    private final Set<String> tools = new HashSet<>();
    private final Set<String> openToolCalls = new HashSet<>();
    private final Set<String> steps = new HashSet<>();
    private final Set<String> finishedSteps = new HashSet<>();
    private final Set<String> finishedRuns = new HashSet<>();
    private final Set<String> failedRuns = new HashSet<>();

    /**
     * Tool calls whose committed REQUESTED event was already turned into a START/ARGS/END
     * sequence. The same event can arrive live and be replayed after reconnect; converting it
     * twice would emit a start-less duplicate END.
     */
    private final Set<String> requestedToolCalls = new HashSet<>();

    /** Argument text already streamed live per call, used to complete a partial preview. */
    private final Map<String, StringBuilder> toolArgumentsPreview = new HashMap<>();

    /**
     * Native journal event ids already delivered as text deltas. The same delta travels twice —
     * once live, once committed — and a reconnected stream replays committed deltas the previous
     * connection may have previewed live. Either arrival wins; the twin is dropped.
     */
    private final Set<String> deliveredDeltas = new HashSet<>();

    /**
     * Terminal tool results already delivered, per call id; the value says whether the delivered
     * result carried real content. One tool call can report several terminal updates (an observed
     * result and a later native placeholder), and a placeholder must not replace the real output.
     */
    private final Map<String, Boolean> toolResults = new HashMap<>();

    public List<AguiEvent> map(SessionStreamEvent event) {
        Objects.requireNonNull(event);
        if (event instanceof SessionStreamEvent.Committed committed)
            return committed(committed.event());
        if (event instanceof SessionStreamEvent.PreviewStarted preview) {
            if (preview.previewType() != EventDeltaType.MESSAGE) return List.of();
            return startMessage(
                    preview.threadId(), require(preview.runId(), "runId"), preview.data());
        }
        if (event instanceof SessionStreamEvent.EventDelta delta) {
            if (delta.deltaType() == EventDeltaType.TOOL_OUTPUT)
                return toolOutput(delta.threadId(), delta.runId(), delta.data());
            if (delta.sourceEventId() != null && deliveredDeltas.contains(delta.sourceEventId()))
                return List.of();
            String runId = require(delta.runId(), "runId");
            List<AguiEvent> result =
                    delta.deltaType() == EventDeltaType.MESSAGE
                            ? messageDelta(delta.threadId(), runId, delta.data())
                            : toolArguments(delta.threadId(), runId, delta.data());
            // Claim the source event only when it produced a frame. A dropped preview (blank
            // placeholder name, empty text) must not suppress its committed twin.
            if (!result.isEmpty() && delta.sourceEventId() != null)
                deliveredDeltas.add(delta.sourceEventId());
            return result;
        }
        return List.of();
    }

    List<AguiEvent> committed(PublicEvent event) {
        Objects.requireNonNull(event);
        return switch (event.type()) {
            case "run.started" -> runStarted(event);
            case "run.ended", "run.lost" -> runEnded(event);
            case "step.started" -> stepStarted(event);
            case "step.ended" -> stepEnded(event);
            case "message.delta" -> messageDelta(event);
            case "message.completed" -> message(event);
            case "model.call.ended" -> stoppedPartial(event);
            case "tool.call.updated" -> tool(event);
            case "turn.waiting" -> waiting(event);
            default -> extension(event);
        };
    }

    /**
     * Application-owned extension events ({@code x.*}) carry no framework state to reconcile.
     * Forward them unchanged as CUSTOM frames so a client reads the same value live and from a
     * replay; the framework stays unaware of their payload.
     */
    private List<AguiEvent> extension(PublicEvent event) {
        if (!event.type().startsWith("x.") || event.runId() == null) return List.of();
        return List.of(
                new AguiEvent.Custom(
                        event.threadId(),
                        event.runId(),
                        event.type(),
                        DurableJson.convert(event.data(), Object.class)));
    }

    List<AguiEvent> live(LiveEvent event) {
        Objects.requireNonNull(event);
        String threadId = event.threadId();
        if (ToolOutputDelta.TYPE.equals(event.type()))
            return toolOutput(threadId, event.runId(), event.data());
        String runId = require(event.runId(), "runId");
        var data = event.data();
        return switch (event.type()) {
            case "message.started" -> startMessage(threadId, runId, data);
            case "message.delta" -> messageDelta(threadId, runId, data);
            case "tool.arguments.delta" -> toolArguments(threadId, runId, data);
            // Result metadata published live by the adapter; the committed copy replays on
            // reconnect. Both reach the client as the same CUSTOM name.
            case "x.result_metadata" ->
                    List.of(
                            new AguiEvent.Custom(
                                    threadId,
                                    runId,
                                    event.type(),
                                    DurableJson.convert(data, Object.class)));
            default -> List.of();
        };
    }

    private static List<AguiEvent> toolOutput(String threadId, String runId, JsonNode data) {
        if (runId == null) return List.of();
        return List.of(
                new AguiEvent.Custom(
                        threadId,
                        runId,
                        "agentscope.tool_output_delta",
                        DurableJson.convert(data, Object.class)));
    }

    private List<AguiEvent> runStarted(PublicEvent event) {
        clear();
        return List.of(new AguiEvent.RunStarted(event.threadId(), require(event.runId(), "runId")));
    }

    private List<AguiEvent> runEnded(PublicEvent event) {
        String status = event.data().path("status").asText();
        if ("STOPPED".equals(status)
                && PARKED_RUN_REASONS.contains(event.data().path("reason").asText()))
            return List.of();
        String runId = require(event.runId(), "runId");
        if ("FAILED".equals(status) || "LOST".equals(status)) {
            if (!failedRuns.add(runId)) return List.of();
            finishedRuns.add(runId);
            var result = new ArrayList<>(closeOpen(event.threadId(), runId));
            result.add(
                    new AguiEvent.RunError(
                            event.threadId(),
                            runId,
                            event.data().path("reason").asText("Durable Run failed"),
                            status));
            return List.copyOf(result);
        }
        if (!finishedRuns.add(runId)) return List.of();
        var result = new ArrayList<>(closeOpen(event.threadId(), runId));
        result.add(
                new AguiEvent.RunFinished(
                        event.threadId(),
                        runId,
                        null,
                        "STOPPED".equals(status)
                                ? new AguiEvent.RunFinishedCancelledOutcome()
                                : new AguiEvent.RunFinishedSuccessOutcome()));
        return List.copyOf(result);
    }

    private List<AguiEvent> stepStarted(PublicEvent event) {
        String stepId = require(event.stepId(), "stepId");
        steps.add(stepId);
        return List.of(
                new AguiEvent.StepStarted(
                        event.threadId(), require(event.runId(), "runId"), stepId));
    }

    private List<AguiEvent> stepEnded(PublicEvent event) {
        String stepId = require(event.stepId(), "stepId");
        String runId = require(event.runId(), "runId");
        if (!finishedSteps.add(runId + "\u0000" + stepId)) return List.of();
        var result = new ArrayList<AguiEvent>();
        closeReasoning(event.threadId(), runId, result);
        if (!steps.remove(stepId)) {
            // Reconnected mid-step: this stream never saw the step's start, but AG-UI requires
            // every STEP_FINISHED to follow a STEP_STARTED on the same connection.
            result.add(new AguiEvent.StepStarted(event.threadId(), runId, stepId));
        }
        result.add(new AguiEvent.StepFinished(event.threadId(), runId, stepId));
        return List.copyOf(result);
    }

    /**
     * A Turn entered WAITING: the Run ends with every pending interaction, because a later {@code
     * run.ended} never arrives while the Turn is parked. Everything the Run opened is closed first.
     */
    private List<AguiEvent> waiting(PublicEvent event) {
        var interactions = event.data().path("interactions");
        String runId = event.runId();
        if (runId == null || !interactions.isArray() || interactions.isEmpty()) return List.of();
        if (!finishedRuns.add(runId)) return List.of();
        var result = new ArrayList<>(closeOpen(event.threadId(), runId));
        var interrupts = new ArrayList<AguiEvent.Interrupt>();
        for (var interaction : interactions) interrupts.add(interrupt(interaction));
        result.add(
                new AguiEvent.RunFinished(
                        event.threadId(),
                        runId,
                        null,
                        new AguiEvent.RunFinishedInterruptOutcome(List.copyOf(interrupts))));
        return List.copyOf(result);
    }

    /** AG-UI requires every step, message and tool call a Run opened to be closed first. */
    private List<AguiEvent> closeOpen(String threadId, String runId) {
        var result = new ArrayList<AguiEvent>();
        closeReasoning(threadId, runId, result);
        for (String messageId : List.copyOf(messages))
            result.add(new AguiEvent.TextMessageEnd(threadId, runId, messageId));
        for (String callId : List.copyOf(openToolCalls))
            result.add(new AguiEvent.ToolCallEnd(threadId, runId, callId));
        // Close the Step last: AG-UI requires messages and tool calls to end before STEP_FINISHED.
        for (String stepId : List.copyOf(steps))
            result.add(new AguiEvent.StepFinished(threadId, runId, stepId));
        messages.clear();
        messageText.clear();
        reasoningMessages.clear();
        openReasoning = null;
        tools.clear();
        openToolCalls.clear();
        steps.clear();
        return List.copyOf(result);
    }

    private void clear() {
        messages.clear();
        messageText.clear();
        reasoningMessages.clear();
        openReasoning = null;
        tools.clear();
        openToolCalls.clear();
        steps.clear();
        finishedSteps.clear();
        finishedRuns.clear();
        failedRuns.clear();
        requestedToolCalls.clear();
        toolArgumentsPreview.clear();
        deliveredDeltas.clear();
        toolResults.clear();
    }

    private static AguiEvent.Interrupt interrupt(JsonNode interaction) {
        String interruptId = interaction.required("id").asText();
        String requestId = interaction.path("requestId").asText(null);
        String kind = interaction.path("kind").asText();
        var call = interaction.path("call");
        String toolName = call.path("name").asText("tool");
        boolean confirmation = "confirmation".equals(kind);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(
                AguiInterruptConstants.METADATA_AGENTSCOPE_INTERRUPT_KIND,
                confirmation ? AguiInterruptConstants.INTERRUPT_KIND_PERMISSION_CONFIRM : kind);
        metadata.put(AguiInterruptConstants.METADATA_TOOL_NAME, toolName);
        metadata.put("automatic", interaction.path("automatic").asBoolean());
        if (requestId != null) metadata.put("requestId", requestId);
        if (call.hasNonNull("arguments"))
            metadata.put(
                    AguiInterruptConstants.METADATA_TOOL_INPUT,
                    DurableJson.convert(call.get("arguments"), Object.class));
        return new AguiEvent.Interrupt(
                interruptId,
                AguiInterruptConstants.TOOL_CALL_INTERRUPT_REASON,
                confirmation
                        ? "Tool '" + toolName + "' requires user confirmation before execution"
                        : "Tool '" + toolName + "' requires external execution",
                requestId,
                confirmation ? CONFIRM_RESPONSE_SCHEMA : null,
                null,
                Collections.unmodifiableMap(metadata));
    }

    private List<AguiEvent> startMessage(String threadId, String runId, JsonNode data) {
        String messageId = data.required("messageId").asText();
        if (data.path("reasoning").asBoolean(false)) {
            if (!reasoningMessages.add(messageId)) return List.of();
            var result = new ArrayList<AguiEvent>();
            closeReasoning(threadId, runId, result);
            openReasoning = messageId;
            result.add(
                    new AguiEvent.ReasoningMessageStart(threadId, runId, messageId, "reasoning"));
            return List.copyOf(result);
        }
        if (!messages.add(messageId)) return List.of();
        messageText.put(messageId, new StringBuilder());
        return List.of(new AguiEvent.TextMessageStart(threadId, runId, messageId, "assistant"));
    }

    /** Closes the open reasoning message; AG-UI requires it before the next visible output. */
    private void closeReasoning(String threadId, String runId, List<AguiEvent> result) {
        if (openReasoning == null) return;
        result.add(new AguiEvent.ReasoningMessageEnd(threadId, runId, openReasoning));
        // Forget the id once it is closed: a model can keep thinking under the same message id
        // after a tool call, and the next content must open a fresh reasoning message instead of
        // arriving without a start frame.
        reasoningMessages.remove(openReasoning);
        openReasoning = null;
    }

    private List<AguiEvent> messageDelta(PublicEvent event) {
        // Same native event may arrive live first (or be replayed after): deliver once.
        if (!deliveredDeltas.add(event.eventId())) return List.of();
        return messageDelta(event.threadId(), require(event.runId(), "runId"), event.data());
    }

    private List<AguiEvent> messageDelta(String threadId, String runId, JsonNode data) {
        String messageId = data.required("messageId").asText();
        String text = data.path("text").asText();
        if (data.path("reasoning").asBoolean(false)) {
            var result = new ArrayList<AguiEvent>();
            if (!reasoningMessages.contains(messageId))
                result.addAll(startMessage(threadId, runId, data));
            result.add(new AguiEvent.ReasoningMessageContent(threadId, runId, messageId, text));
            return List.copyOf(result);
        }
        var result = new ArrayList<AguiEvent>();
        closeReasoning(threadId, runId, result);
        if (!messages.contains(messageId)) result.addAll(startMessage(threadId, runId, data));
        messageText.get(messageId).append(text);
        result.add(new AguiEvent.TextMessageContent(threadId, runId, messageId, text));
        return List.copyOf(result);
    }

    private List<AguiEvent> message(PublicEvent event) {
        JsonNode message = event.data().required("message");
        if (!"assistant".equals(message.path("role").asText()) || event.runId() == null)
            return List.of();
        String messageId = event.data().required("messageId").asText();
        String text = text(message.required("content"));
        var result = new ArrayList<AguiEvent>();
        closeReasoning(event.threadId(), event.runId(), result);
        // A tool-only assistant turn carries no visible text. Opening a text message here would
        // leave an empty bubble; the tool card is emitted by tool.call.updated instead. If a
        // preview already opened the message, still close it.
        if (text.isEmpty()) {
            if (!messages.contains(messageId)) return List.copyOf(result);
            result.add(new AguiEvent.TextMessageEnd(event.threadId(), event.runId(), messageId));
            messages.remove(messageId);
            messageText.remove(messageId);
            return List.copyOf(result);
        }
        if (!messages.contains(messageId))
            result.addAll(startMessage(event.threadId(), event.runId(), event.data()));
        String preview = messageText.get(messageId).toString();
        if (text.startsWith(preview) && text.length() > preview.length())
            result.add(
                    new AguiEvent.TextMessageContent(
                            event.threadId(),
                            event.runId(),
                            messageId,
                            text.substring(preview.length())));
        else if (!text.equals(preview))
            result.add(
                    new AguiEvent.Custom(
                            event.threadId(),
                            event.runId(),
                            "agentscope.message_reconciled",
                            DurableJson.convert(message, Object.class)));
        result.add(new AguiEvent.TextMessageEnd(event.threadId(), event.runId(), messageId));
        messages.remove(messageId);
        messageText.remove(messageId);
        return List.copyOf(result);
    }

    /**
     * Renders a stopped model call's assembled partial as a closed text message. It shares the
     * Step's message id so it reconciles with the live preview instead of duplicating it: a
     * client that already showed the text only closes the bubble, while a reconnect that never
     * saw the deltas opens it from this committed fact.
     */
    private List<AguiEvent> stoppedPartial(PublicEvent event) {
        if (event.runId() == null) return List.of();
        var partial = event.data().path("partial");
        String text = partial.path("text").asText("");
        if (!partial.isObject() || text.isEmpty()) return List.of();
        String messageId = event.data().path("messageId").asText(null);
        if (messageId == null || messageId.isEmpty())
            messageId = "stopped:" + event.data().path("modelCallId").asText(event.eventId());
        var result = new ArrayList<AguiEvent>();
        closeReasoning(event.threadId(), event.runId(), result);
        if (!messages.contains(messageId))
            result.addAll(
                    startMessage(
                            event.threadId(),
                            event.runId(),
                            DurableJson.value(Map.of("messageId", messageId))));
        String preview = messageText.get(messageId).toString();
        if (text.startsWith(preview) && text.length() > preview.length())
            result.add(
                    new AguiEvent.TextMessageContent(
                            event.threadId(),
                            event.runId(),
                            messageId,
                            text.substring(preview.length())));
        else if (!text.equals(preview))
            result.add(
                    new AguiEvent.TextMessageContent(
                            event.threadId(), event.runId(), messageId, text));
        result.add(new AguiEvent.TextMessageEnd(event.threadId(), event.runId(), messageId));
        messages.remove(messageId);
        messageText.remove(messageId);
        return List.copyOf(result);
    }

    private static boolean isCompleteJson(String value) {
        try {
            DurableJson.parse(value);
            return true;
        } catch (RuntimeException error) {
            return false;
        }
    }

    private List<AguiEvent> toolArguments(String threadId, String runId, JsonNode data) {
        String callId = data.required("toolCallId").asText();
        String name = data.path("name").asText("");
        // Streamed fragments often carry a placeholder name (__fragment__) until the model emits
        // the real tool name. Starting the card here would pin that placeholder: the committed
        // tool.call.updated cannot rename an already-open tool call. Wait for REQUESTED instead.
        if (name.isBlank() || name.startsWith("__")) return List.of();
        var result = new ArrayList<AguiEvent>();
        // A tool call is visible output: finish the reasoning message before opening it.
        closeReasoning(threadId, runId, result);
        if (tools.add(callId))
            result.add(new AguiEvent.ToolCallStart(threadId, runId, callId, name));
        openToolCalls.add(callId);
        var text = data.path("text").asText();
        toolArgumentsPreview.computeIfAbsent(callId, ignored -> new StringBuilder()).append(text);
        result.add(new AguiEvent.ToolCallArgs(threadId, runId, callId, text));
        return List.copyOf(result);
    }

    private List<AguiEvent> tool(PublicEvent event) {
        if (event.runId() == null) return List.of();
        var data = event.data();
        if (data.path("applied").asBoolean() && !data.has("content")) return List.of();
        String callId = data.required("callId").asText();
        String status = data.required("status").asText();
        var result = new ArrayList<AguiEvent>();
        if ("REQUESTED".equals(status)) {
            // A duplicate committed request (live then replay, or reconnect replay) must not emit
            // a second END for a call whose START is already gone.
            if (!requestedToolCalls.add(callId)) return List.of();
            // The tool card is visible output: close reasoning before starting it.
            closeReasoning(event.threadId(), event.runId(), result);
            if (tools.add(callId))
                result.add(
                        new AguiEvent.ToolCallStart(
                                event.threadId(),
                                event.runId(),
                                callId,
                                data.path("name").asText("tool")));
            // Live preview already streamed some or all arguments. Sending the complete JSON
            // again makes an append-style client concatenate both copies into invalid JSON. Send
            // only the missing suffix: a preview can be truncated (fragments are best effort) or
            // absent after reconnect, and the committed value is then the only complete source.
            if (data.has("arguments")) {
                String committed = data.get("arguments").toString();
                var preview = toolArgumentsPreview.get(callId);
                String previewText = preview == null ? "" : preview.toString();
                String missing = null;
                if (previewText.isEmpty()) {
                    missing = committed;
                } else if (committed.startsWith(previewText)) {
                    missing = committed.substring(previewText.length());
                } else if (!isCompleteJson(previewText)) {
                    // A truncated preview cannot be extended by an append-style client without
                    // corrupting the JSON, and the protocol has no replace frame. The committed
                    // call still executes with its full arguments.
                    missing = null;
                }
                // A complete preview may differ only in whitespace; sending the committed copy
                // would duplicate the arguments.
                if (missing != null && !missing.isEmpty())
                    result.add(
                            new AguiEvent.ToolCallArgs(
                                    event.threadId(), event.runId(), callId, missing));
            }
            toolArgumentsPreview.remove(callId);
            result.add(new AguiEvent.ToolCallEnd(event.threadId(), event.runId(), callId));
            openToolCalls.remove(callId);
        } else if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "DENIED", "UNKNOWN")
                .contains(status)) {
            String observed = toolContent(data);
            Boolean delivered = toolResults.get(callId);
            // Deliver one terminal result per call. A second update may only replace a placeholder
            // that carried no content; it must never overwrite a real output.
            if (delivered != null && (delivered || observed == null)) return List.of();
            toolResults.put(callId, observed != null);
            String content = observed != null ? observed : data.path("reason").asText(status);
            result.add(
                    new AguiEvent.ToolCallResult(
                            event.threadId(),
                            event.runId(),
                            callId,
                            content,
                            "tool",
                            callId + ":result"));
            tools.remove(callId);
            openToolCalls.remove(callId);
        }
        return List.copyOf(result);
    }

    /**
     * Real output carried by a terminal tool update, or {@code null} when the update only reports
     * a status. An empty array is what a placeholder result serializes to, so it counts as absent.
     */
    private static String toolContent(JsonNode data) {
        JsonNode content = data.get("content");
        if (content == null || content.isNull()) return null;
        if (content.isArray() && content.isEmpty()) return null;
        String value = content.toString();
        return value.isBlank() || "[]".equals(value) || "null".equals(value) ? null : value;
    }

    private static String text(JsonNode content) {
        var result = new StringBuilder();
        for (var block : content)
            if ("text".equals(block.path("type").asText()))
                result.append(block.path("text").asText());
        return result.toString();
    }

    private static String require(String value, String name) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("AG-UI event requires " + name);
        return value;
    }
}
