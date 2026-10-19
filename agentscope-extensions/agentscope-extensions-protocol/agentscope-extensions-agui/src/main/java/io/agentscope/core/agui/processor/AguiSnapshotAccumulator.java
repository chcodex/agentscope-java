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
import io.agentscope.core.agui.model.AguiFunctionCall;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.model.AguiToolCall;
import io.agentscope.core.agui.model.MessageContent;
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import io.agentscope.core.agui.store.AguiThreadSnapshot.ActivityEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Materializes AG-UI events of one run into a presentation snapshot ({@code messages}, {@code
 * state}, {@code activity}).
 *
 * <p>The accumulator is the state counterpart of the adapter projection: the adapter streams {@link
 * AguiEvent}s only, and the accumulator folds those events into an {@link AguiThreadSnapshot}
 * without storing an event sequence. All mutating methods are synchronized because the materialized
 * run can be shared across conversion paths and terminal hooks.
 *
 * <p>Interrupt-suppression semantics (see issues #2911 / #2920): resolved historical interrupts
 * never materialize here because only the latest covering snapshot is kept; tool calls observed
 * with {@code TOOL_CALL_START} but no {@code TOOL_CALL_RESULT} are closed with a synthetic pending
 * tool call (an assistant message carrying the tool call plus a tool message without content) so
 * no tool is presented as stuck.
 */
final class AguiSnapshotAccumulator {

    private static final String ROLE_TOOL = "tool";

    private final String threadId;
    private final String runId;
    private final AguiThreadSnapshot baseSnapshot;

    private final Set<String> knownMessageIds = new LinkedHashSet<>();
    private final List<AguiMessage> baseMessages = new ArrayList<>();
    private final List<AguiMessage> observedInputMessages = new ArrayList<>();

    private final Map<String, InFlightMessage> inFlight = new LinkedHashMap<>();
    private final List<AguiMessage> emittedMessages = new ArrayList<>();

    private final Map<String, String> startedToolCalls = new LinkedHashMap<>();
    private final Set<String> resolvedToolCallIds = new LinkedHashSet<>();
    private final Map<String, AguiMessage> toolResultMessages = new LinkedHashMap<>();

    private final Map<String, ActivityEntry> activity = new LinkedHashMap<>();
    private Map<String, Object> state;

    private boolean runFinishedSeen;
    private boolean runErrorSeen;

    AguiSnapshotAccumulator(String threadId, String runId, AguiThreadSnapshot baseSnapshot) {
        this.threadId = threadId;
        this.runId = runId;
        this.baseSnapshot = baseSnapshot;

        if (baseSnapshot != null) {
            baseMessages.addAll(baseSnapshot.messages());
            this.state = AguiJsonPatch.apply(baseSnapshot.state(), null);
            this.activity.putAll(baseSnapshot.activity());
            for (AguiMessage message : baseMessages) {
                if (message != null && message.getId() != null) {
                    knownMessageIds.add(message.getId());
                }
            }
        }
    }

    synchronized void observe(AguiEvent event) {
        if (event == null) {
            return;
        }
        if (event instanceof AguiEvent.RunStarted runStarted) {
            appendInputMessages(
                    runStarted.input() != null ? runStarted.input().getMessages() : null);
        } else if (event instanceof AguiEvent.TextMessageStart textStart) {
            inFlight.put(textStart.messageId(), new InFlightMessage(textStart.role()));
        } else if (event instanceof AguiEvent.TextMessageContent textContent) {
            inFlightText(textContent.messageId(), "assistant").append(textContent.delta());
        } else if (event instanceof AguiEvent.TextMessageEnd textEnd) {
            closeMessage(textEnd.messageId());
        } else if (event instanceof AguiEvent.ReasoningMessageStart reasoningStart) {
            inFlight.put(reasoningStart.messageId(), new InFlightMessage(reasoningStart.role()));
        } else if (event instanceof AguiEvent.ReasoningMessageContent reasoningContent) {
            inFlightText(reasoningContent.messageId(), "reasoning")
                    .append(reasoningContent.delta());
        } else if (event instanceof AguiEvent.ReasoningMessageEnd reasoningEnd) {
            closeMessage(reasoningEnd.messageId());
        } else if (event instanceof AguiEvent.ToolCallStart toolCallStart) {
            startedToolCalls.put(toolCallStart.toolCallId(), toolCallStart.toolCallName());
        } else if (event instanceof AguiEvent.ToolCallResult toolCallResult) {
            recordToolResult(toolCallResult);
        } else if (event instanceof AguiEvent.StateSnapshot stateSnapshot) {
            state = AguiJsonPatch.apply(stateSnapshot.snapshot(), null);
        } else if (event instanceof AguiEvent.StateDelta stateDelta) {
            state = AguiJsonPatch.apply(state, stateDelta.delta());
        } else if (event instanceof AguiEvent.ActivitySnapshot activitySnapshot) {
            AguiActivityConverter.applySnapshot(activity, activitySnapshot);
        } else if (event instanceof AguiEvent.ActivityDelta activityDelta) {
            AguiActivityConverter.applyDelta(activity, activityDelta);
        } else if (event instanceof AguiEvent.RunFinished) {
            runFinishedSeen = true;
        } else if (event instanceof AguiEvent.RunError) {
            runErrorSeen = true;
        }
    }

    synchronized boolean isRunFinished() {
        return runFinishedSeen;
    }

    synchronized boolean isRunError() {
        return runErrorSeen;
    }

    /**
     * Builds the merged snapshot for this thread.
     *
     * <p>The result is the base snapshot (previous thread presentation state) merged with the
     * events observed by this run, plus synthetic dangling-tool-call closures. Callers invoke this
     * only for terminal, non-errored runs.
     *
     * @return the merged snapshot; never null
     */
    synchronized AguiThreadSnapshot materialize() {
        List<AguiMessage> merged = new ArrayList<>();
        Set<String> mergedIds = new LinkedHashSet<>();
        Map<String, Integer> toolCallIdIndex = new HashMap<>();

        appendAll(baseMessages, merged, mergedIds, toolCallIdIndex);
        appendAll(observedInputMessages, merged, mergedIds, toolCallIdIndex);
        appendAll(emittedMessages, merged, mergedIds, toolCallIdIndex);
        for (AguiMessage toolMessage : toolResultMessages.values()) {
            if (!replaceToolResult(merged, toolCallIdIndex, toolMessage)) {
                appendMessage(toolMessage, merged, mergedIds, toolCallIdIndex);
            }
        }
        closeDanglingToolCalls(merged, mergedIds);

        Map<String, Object> materializedState = state != null ? state : baseState();
        return AguiThreadSnapshot.of(merged, materializedState, activity);
    }

    String getThreadId() {
        return threadId;
    }

    String getRunId() {
        return runId;
    }

    // ------------------------------------------------------------------
    // materialization helpers
    // ------------------------------------------------------------------

    private void appendInputMessages(List<AguiMessage> inputMessages) {
        if (inputMessages == null) {
            return;
        }
        for (AguiMessage message : inputMessages) {
            if (message == null || message.getId() == null) {
                continue;
            }
            if (knownMessageIds.add(message.getId())) {
                observedInputMessages.add(message);
            }
        }
    }

    private InFlightMessage inFlightText(String messageId, String role) {
        return inFlight.computeIfAbsent(messageId, ignored -> new InFlightMessage(role));
    }

    private void closeMessage(String messageId) {
        InFlightMessage message = inFlight.remove(messageId);
        if (message == null) {
            return;
        }
        String text = message.text();
        emittedMessages.add(
                new AguiMessage(
                        messageId,
                        message.role(),
                        text.isEmpty() ? null : new MessageContent.Text(text),
                        null,
                        null));
        knownMessageIds.add(messageId);
    }

    private void recordToolResult(AguiEvent.ToolCallResult event) {
        String toolCallId = event.toolCallId();
        resolvedToolCallIds.add(toolCallId);
        String messageId = event.messageId() != null ? event.messageId() : runId + ":" + toolCallId;
        toolResultMessages.put(toolCallId, toolMessage(messageId, toolCallId, event.content()));
    }

    private static AguiMessage toolMessage(String messageId, String toolCallId, String content) {
        return new AguiMessage(
                messageId,
                ROLE_TOOL,
                content != null && !content.isEmpty() ? new MessageContent.Text(content) : null,
                null,
                toolCallId);
    }

    private static void appendAll(
            List<AguiMessage> source,
            List<AguiMessage> merged,
            Set<String> mergedIds,
            Map<String, Integer> toolCallIdIndex) {
        if (source == null) {
            return;
        }
        for (AguiMessage message : source) {
            appendMessage(message, merged, mergedIds, toolCallIdIndex);
        }
    }

    private static void appendMessage(
            AguiMessage message,
            List<AguiMessage> merged,
            Set<String> mergedIds,
            Map<String, Integer> toolCallIdIndex) {
        if (message == null) {
            return;
        }
        boolean toolMessage = ROLE_TOOL.equalsIgnoreCase(message.getRole());
        if (toolMessage && replaceToolResult(merged, toolCallIdIndex, message)) {
            return;
        }
        if (mergedIds.contains(message.getId())) {
            return;
        }
        merged.add(message);
        if (message.getId() != null) {
            mergedIds.add(message.getId());
        }
        if (toolMessage && message.getToolCallId() != null) {
            toolCallIdIndex.put(message.getToolCallId(), merged.size() - 1);
        }
    }

    /** Replaces an already stored tool message with the same toolCallId, keeping its job order. */
    private static boolean replaceToolResult(
            List<AguiMessage> merged,
            Map<String, Integer> toolCallIdIndex,
            AguiMessage toolMessage) {
        Integer position = toolCallIdIndex.get(toolMessage.getToolCallId());
        if (position == null) {
            return false;
        }
        merged.set(position, toolMessage);
        return true;
    }

    /**
     * Closes tool calls that started but had no {@code TOOL_CALL_RESULT} with synthetic pending
     * tool-call messages, so no tool is presented as stuck.
     */
    private void closeDanglingToolCalls(List<AguiMessage> merged, Set<String> mergedIds) {
        List<AguiToolCall> danglingToolCalls = new ArrayList<>();
        List<AguiMessage> danglingResults = new ArrayList<>();
        for (Map.Entry<String, String> entry : startedToolCalls.entrySet()) {
            String toolCallId = entry.getKey();
            if (resolvedToolCallIds.contains(toolCallId)) {
                continue;
            }
            String name =
                    entry.getValue() != null && !entry.getValue().isBlank()
                            ? entry.getValue()
                            : "unknown";
            danglingToolCalls.add(new AguiToolCall(toolCallId, new AguiFunctionCall(name, "{}")));
            danglingResults.add(toolMessage(runId + ":" + toolCallId, toolCallId, null));
        }
        if (danglingToolCalls.isEmpty()) {
            return;
        }
        String assistantId = runId + ":tool-calls";
        if (mergedIds.add(assistantId)) {
            merged.add(new AguiMessage(assistantId, "assistant", null, danglingToolCalls, null));
        }
        merged.addAll(danglingResults);
    }

    private Map<String, Object> baseState() {
        return baseSnapshot != null ? baseSnapshot.state() : Map.of();
    }

    /** Streaming text or reasoning message under assembly. */
    private static final class InFlightMessage {

        private final String role;
        private final StringBuilder text = new StringBuilder();

        InFlightMessage(String role) {
            this.role = role != null && !role.isBlank() ? role : "assistant";
        }

        void append(String delta) {
            if (delta != null) {
                text.append(delta);
            }
        }

        String text() {
            return text.toString();
        }

        String role() {
            return role;
        }
    }
}
