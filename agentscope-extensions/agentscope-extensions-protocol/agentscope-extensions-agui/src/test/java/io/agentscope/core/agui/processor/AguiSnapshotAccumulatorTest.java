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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AguiSnapshotAccumulator} materialization. */
@Tag("unit")
@DisplayName("AguiSnapshotAccumulator Unit Tests")
class AguiSnapshotAccumulatorTest {

    private static RunAgentInput input(List<AguiMessage> messages) {
        return RunAgentInput.builder()
                .threadId("thread-1")
                .runId("run-1")
                .messages(messages)
                .build();
    }

    private static void observeAll(AguiSnapshotAccumulator accumulator, AguiEvent... events) {
        for (AguiEvent event : events) {
            accumulator.observe(event);
        }
    }

    @Test
    @DisplayName("Text messages materialize into the transcript")
    void textMessagesMaterialize() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.RunStarted("thread-1", "run-1", null, input(List.of())),
                new AguiEvent.TextMessageStart("thread-1", "run-1", "msg-1", "assistant"),
                new AguiEvent.TextMessageContent("thread-1", "run-1", "msg-1", "he"),
                new AguiEvent.TextMessageContent("thread-1", "run-1", "msg-1", "llo"),
                new AguiEvent.TextMessageEnd("thread-1", "run-1", "msg-1"),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(List.of(AguiMessage.assistantMessage("msg-1", "hello")), snapshot.messages());
        assertTrue(accumulator.isRunFinished());
        assertFalse(accumulator.isRunError());
    }

    @Test
    @DisplayName("Run-started input messages extend the transcript without duplicates")
    void inputMessagesAppendOnce() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        AguiMessage user = AguiMessage.userMessage("msg-1", "hello");
        observeAll(
                accumulator,
                new AguiEvent.RunStarted("thread-1", "run-1", null, input(List.of(user))),
                new AguiEvent.RunFinished("thread-1", "run-1"),
                // A second run on the same thread replays the full transcript again.
                new AguiEvent.RunStarted(
                        "thread-1",
                        "run-2",
                        null,
                        input(List.of(user, AguiMessage.assistantMessage("msg-2", "hi")))));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(
                List.of(user, AguiMessage.assistantMessage("msg-2", "hi")), snapshot.messages());
    }

    @Test
    @DisplayName("Tool results become tool messages, results for the same call overwrite")
    void toolResultsMaterialize() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.ToolCallStart("thread-1", "run-1", "call-1", "search"),
                new AguiEvent.ToolCallResult(
                        "thread-1", "run-1", "call-1", "result-1", "tool", "reply-1:call-1"),
                new AguiEvent.ToolCallResult(
                        "thread-1", "run-1", "call-1", "result-2", "tool", "reply-2:call-1"),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(1, snapshot.messages().size());
        AguiMessage tool = snapshot.messages().get(0);
        assertEquals("result-2", tool.getTextContent());
        assertEquals("call-1", tool.getToolCallId());
    }

    @Test
    @DisplayName("Dangling started tool calls are closed with synthetic pending messages")
    void danglingToolCallsAreClosed() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.ToolCallStart("thread-1", "run-1", "call-1", "search"),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(2, snapshot.messages().size());
        AguiMessage assistant = snapshot.messages().get(0);
        assertTrue(assistant.isAssistantMessage());
        assertEquals(1, assistant.getToolCalls().size());
        assertEquals("call-1", assistant.getToolCalls().get(0).getId());
        AguiMessage pending = snapshot.messages().get(1);
        assertTrue(pending.isToolMessage());
        assertEquals("call-1", pending.getToolCallId());
        assertNull(pending.getTextContent());
    }

    @Test
    @DisplayName("Reasoning messages materialize with role reasoning")
    void reasoningMessagesMaterialize() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.ReasoningMessageStart("thread-1", "run-1", "msg-1", "reasoning"),
                new AguiEvent.ReasoningMessageContent("thread-1", "run-1", "msg-1", "thinking"),
                new AguiEvent.ReasoningMessageEnd("thread-1", "run-1", "msg-1"),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(
                List.of(AguiMessage.reasoningMessage("msg-1", "thinking")), snapshot.messages());
    }

    @Test
    @DisplayName("State snapshots replace, state deltas patch the current value")
    void stateEventsMaterialize() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.StateSnapshot(
                        "thread-1", "run-1", Map.of("weather", Map.of("temp", 20))),
                new AguiEvent.StateDelta(
                        "thread-1",
                        "run-1",
                        List.of(
                                AguiEvent.JsonPatchOperation.replace("/weather/temp", 25),
                                AguiEvent.JsonPatchOperation.add("/cursor", 3))),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();
        Map<?, ?> weather = (Map<?, ?>) snapshot.state().get("weather");

        assertEquals(25, weather.get("temp"));
        assertEquals(3, snapshot.state().get("cursor"));
    }

    @Test
    @DisplayName("Activity snapshots and deltas materialize per message id")
    void activityEventsMaterialize() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.ActivitySnapshot(
                        "thread-1", "run-1", "msg-1", "map", Map.of("progress", 40)),
                new AguiEvent.ActivityDelta(
                        "thread-1",
                        "run-1",
                        "msg-1",
                        "map",
                        List.of(AguiEvent.JsonPatchOperation.replace("/progress", 80))),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();
        AguiThreadSnapshot.ActivityEntry entry = snapshot.activity().get("msg-1");

        assertEquals("map", entry.activityType());
        assertEquals(80, entry.content().get("progress"));
    }

    @Test
    @DisplayName("Materialization merges with the base snapshot state")
    void mergesWithBaseSnapshot() {
        AguiThreadSnapshot base =
                AguiThreadSnapshot.of(
                        List.of(AguiMessage.userMessage("msg-0", "earlier")),
                        Map.of("old", true),
                        Map.of(
                                "msg-0",
                                new AguiThreadSnapshot.ActivityEntry("map", Map.of("p", 1))));
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", base);
        observeAll(
                accumulator,
                new AguiEvent.RunStarted(
                        "thread-1",
                        "run-1",
                        null,
                        input(List.of(AguiMessage.userMessage("msg-1", "hello")))),
                new AguiEvent.TextMessageStart("thread-1", "run-1", "msg-2", "assistant"),
                new AguiEvent.TextMessageContent("thread-1", "run-1", "msg-2", "welcome"),
                new AguiEvent.TextMessageEnd("thread-1", "run-1", "msg-2"),
                new AguiEvent.StateDelta(
                        "thread-1",
                        "run-1",
                        List.of(AguiEvent.JsonPatchOperation.add("/fresh", true))),
                new AguiEvent.ActivitySnapshot("thread-1", "run-1", "msg-2", "map", Map.of("p", 2)),
                new AguiEvent.RunFinished("thread-1", "run-1"));

        AguiThreadSnapshot snapshot = accumulator.materialize();

        assertEquals(
                List.of(
                        AguiMessage.userMessage("msg-0", "earlier"),
                        AguiMessage.userMessage("msg-1", "hello"),
                        AguiMessage.assistantMessage("msg-2", "welcome")),
                snapshot.messages());
        assertEquals(true, snapshot.state().get("old"));
        assertEquals(true, snapshot.state().get("fresh"));
        assertEquals(2, snapshot.activity().size());
    }

    @Test
    @DisplayName("RunError is observable and prevents save flows from reading a finished run")
    void runErrorFlag() {
        AguiSnapshotAccumulator accumulator =
                new AguiSnapshotAccumulator("thread-1", "run-1", null);
        observeAll(
                accumulator,
                new AguiEvent.RunStarted("thread-1", "run-1", null, input(List.of())),
                new AguiEvent.RunError("thread-1", "run-1", "boom", "INTERNAL_ERROR"));

        assertTrue(accumulator.isRunError());
        assertFalse(accumulator.isRunFinished());
    }
}
