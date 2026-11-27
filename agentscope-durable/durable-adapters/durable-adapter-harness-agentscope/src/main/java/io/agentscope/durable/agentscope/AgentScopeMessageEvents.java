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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.agentscope.core.journal.JournalEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.durable.DurableJson;
import io.agentscope.durable.DurableTypes.EventInput;
import io.agentscope.durable.DurableTypes.LiveEventInput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Rebuilt from the native journal; commit a staged copy together with its public events. */
final class AgentScopeMessageEvents {
    record Output(EventInput durable, List<LiveEventInput> live) {
        private static final Output NONE = new Output(null, List.of());

        Output {
            live = List.copyOf(live);
        }

        static Output none() {
            return NONE;
        }

        static Output durable(EventInput event, LiveEventInput... live) {
            return new Output(event, List.of(live));
        }

        static Output live(LiveEventInput... events) {
            return new Output(null, List.of(events));
        }
    }

    private record Step(String messageId, String phase) {}

    private record Reference(String messageId, long attempt) {}

    private record Version(long attempt, long revision, String replyId, String completedHash) {}

    private final Map<String, Step> steps = new HashMap<>();
    private final Map<String, Reference> calls = new HashMap<>();
    private final Map<String, Reference> nativeMessages = new HashMap<>();
    private final Map<String, Version> versions = new HashMap<>();

    /** Real tool names seen on a model chunk, keyed by tool call id. */
    private final Map<String, String> toolNames = new HashMap<>();

    AgentScopeMessageEvents() {}

    AgentScopeMessageEvents(AgentScopeMessageEvents source) {
        steps.putAll(source.steps);
        calls.putAll(source.calls);
        nativeMessages.putAll(source.nativeMessages);
        versions.putAll(source.versions);
        toolNames.putAll(source.toolNames);
    }

    String messageId(String turnId, Msg message) {
        return messageId(turnId, message.getId());
    }

    String messageId(String turnId, String nativeId) {
        String key = nativeKey(turnId, nativeId);
        var reference = nativeMessages.get(key);
        return reference == null ? "message:" + key : reference.messageId();
    }

    Output accept(JournalEvent event) {
        var data = DurableJson.value(event.data());
        String stepId = data.path("stepId").asText();
        if ("step/start".equals(event.type())) {
            steps.put(
                    stepId,
                    new Step(
                            "message:"
                                    + DurableJson.hash(
                                            List.of(
                                                    event.turnId(),
                                                    "reasoning",
                                                    data.path("iteration").asInt())),
                            data.path("phase").asText()));
        } else if ("step/phase".equals(event.type()) && steps.containsKey(stepId)) {
            steps.put(stepId, new Step(steps.get(stepId).messageId(), data.path("phase").asText()));
        } else if ("request/prepared".equals(event.type())
                && "REASONING".equals(data.path("purpose").asText())
                && steps.containsKey(stepId)) {
            String id = steps.get(stepId).messageId();
            String callId = data.path("modelCallId").asText();
            // The collecting host re-enters the same queued model call. Its first presentation
            // delta advances the revision of the existing attempt.
            var previous = versions.get(id);
            var collecting = calls.get(callId);
            if (collecting != null
                    && collecting.messageId().equals(id)
                    && previous != null
                    && collecting.attempt() == previous.attempt()) return Output.none();
            long attempt = previous == null ? 1 : Math.addExact(previous.attempt(), 1);
            versions.put(id, new Version(attempt, 0, null, null));
            calls.put(callId, new Reference(id, attempt));
        } else if ("model/response".equals(event.type())) {
            var reference = calls.get(data.path("modelCallId").asText());
            String responseId = data.path("responseId").asText("");
            if (reference != null && !responseId.isEmpty())
                nativeMessages.put(nativeKey(event.turnId(), responseId), reference);
        } else if ("model/chunk".equals(event.type())) {
            var chunk = data.path("chunk");
            // The first tool-call chunk carries the real name; later chunks are argument
            // fragments with a blank name. Keep the real name so presentation deltas can use it
            // instead of the accumulator's __fragment__ placeholder.
            for (var block : chunk.path("content")) {
                String callId = block.path("id").asText("");
                String toolName = block.path("name").asText("");
                if (!callId.isBlank() && !toolName.isBlank() && !toolName.startsWith("__")) {
                    toolNames.put(callId, toolName);
                }
            }
            var reference = calls.get(data.path("modelCallId").asText());
            if (reference != null && data.path("chunk").hasNonNull("id"))
                nativeMessages.put(
                        nativeKey(event.turnId(), data.path("chunk").path("id").asText()),
                        reference);
        } else if ("presentation/text".equals(event.type())
                || "presentation/tool_arguments".equals(event.type())) {
            var step = steps.get(stepId);
            if (step == null || !"REASONING".equals(step.phase())) return Output.none();
            var version = versions.get(step.messageId());
            if (version == null || version.completedHash() != null) return Output.none();
            String replyId = data.path("replyId").asText();
            long revision = version.revision();
            boolean started = version.replyId() == null || !version.replyId().equals(replyId);
            // Replaying a committed model response can restart its presentation without another
            // model request. Replace the old partial output instead of appending it again.
            if (version.replyId() != null && !version.replyId().equals(replyId))
                revision = Math.addExact(revision, 1);
            version = new Version(version.attempt(), revision, replyId, null);
            versions.put(step.messageId(), version);
            if ("presentation/tool_arguments".equals(event.type())) {
                var output = payload(step.messageId(), version, data.path("text").asText());
                String callId = data.path("toolCallId").asText("");
                if (!callId.isBlank()) output.put("toolCallId", callId);
                String toolName = data.path("name").asText("");
                if (toolName.isBlank() || toolName.startsWith("__")) {
                    // Argument fragments carry __fragment__; use the name captured from the
                    // first model chunk so live frames open the card with the real tool name.
                    toolName = toolNames.getOrDefault(callId, "");
                }
                if (!toolName.isBlank()) output.put("name", toolName);
                return Output.live(live(event, "tool.arguments.delta", output));
            }
            var output = payload(step.messageId(), version, data.path("text").asText());
            if (data.hasNonNull("blockId")) output.set("blockId", data.get("blockId"));
            var delta = live(event, "message.delta", output);
            // Streaming deltas stay live-only: the committed message.completed carries the full
            // text, and a reconnect resumes the current Step from the in-process live buffer
            // instead of every token being written to durable history.
            if (!started) return Output.live(delta);
            var start = payload(step.messageId(), version, "");
            start.remove("text");
            return Output.live(live(event, ":start", "message.started", start), delta);
        } else if ("presentation/thinking".equals(event.type())) {
            var step = steps.get(stepId);
            if (step == null) return Output.none();
            // A real ThinkingBlock delta. It uses its own message id so the reasoning stream and
            // the visible reply never share a message id. Reasoning is a preview too, so it stays
            // live-only until a committed message carries it.
            String id = "thinking:" + DurableJson.hash(List.of(event.turnId(), "thinking", stepId));
            var output = payload(id, new Version(1, 0, null, null), data.path("text").asText());
            output.put("reasoning", true);
            return Output.live(live(event, "message.delta", output));
        } else if ("message/assistant".equals(event.type()) || "run/output".equals(event.type())) {
            Msg message =
                    JsonUtils.getJsonCodec().convertValue(event.data().get("message"), Msg.class);
            String id = messageId(event.turnId(), message);
            var publicMessage = AgentScopeMessages.output(message, id);
            if (publicMessage.content().isEmpty()
                    && (!message.hasContentBlocks(ToolUseBlock.class)
                            || "run/output".equals(event.type()))) return Output.none();
            String hash = DurableJson.hash(publicMessage);
            var version = versions.getOrDefault(id, new Version(0, 0, null, null));
            var reference = nativeMessages.get(nativeKey(event.turnId(), message.getId()));
            if (reference != null && reference.attempt() < version.attempt()) return Output.none();
            if (Objects.equals(hash, version.completedHash())) return Output.none();
            long revision =
                    version.completedHash() == null
                            ? version.revision()
                            : Math.addExact(version.revision(), 1);
            version = new Version(version.attempt(), revision, version.replyId(), hash);
            versions.put(id, version);
            var output = payload(id, version, message.getTextContent());
            output.set("message", DurableJson.value(publicMessage));
            return Output.durable(event(event, "message.completed", output));
        }
        return Output.none();
    }

    private static String nativeKey(String turnId, String nativeId) {
        return DurableJson.hash(List.of(turnId, nativeId));
    }

    private static ObjectNode payload(String id, Version version, String text) {
        return (ObjectNode)
                DurableJson.value(
                        Map.of(
                                "messageId",
                                id,
                                "attempt",
                                version.attempt(),
                                "revision",
                                version.revision(),
                                "text",
                                text == null ? "" : text));
    }

    private static EventInput event(JournalEvent source, String type, ObjectNode data) {
        return new EventInput(
                source.eventId(),
                type,
                data,
                source.executionRunId(),
                (String) source.data().get("stepId"));
    }

    private static LiveEventInput live(JournalEvent source, String type, ObjectNode data) {
        return live(source, "", type, data);
    }

    private static LiveEventInput live(
            JournalEvent source, String idSuffix, String type, ObjectNode data) {
        return new LiveEventInput(
                source.eventId() + idSuffix,
                source.occurredAt(),
                type,
                data,
                source.executionRunId(),
                (String) source.data().get("stepId"));
    }
}
