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

import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.THREAD_ID;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.append;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.assistantToolCall;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.message;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.modelChunk;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.textBlock;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.toolResult;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.toolResultBlock;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.toolUse;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.toolUseBlock;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.user;
import static io.agentscope.core.agui.processor.AguiSessionLogTestSupport.writer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agui.adapter.AguiAdapterConfig;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.event.AguiFramedEvent;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.session.SessionLog;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Attach stream tests for the SessionLog-backed AG-UI recovery path. */
class AguiSessionLogAttachTest {

    private static final AguiAdapterConfig CONFIG = AguiAdapterConfig.defaultConfig();
    private static final Duration POLL = Duration.ofMillis(10);

    @Test
    void attachFinishedRunDrainsNothingAndFinishes() {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", Map.of("executionRunId", "exec-1"));
            append(log, writer, "message/user", message(user("u1", "hello")));
            append(log, writer, "run/end", Map.of("status", "completed"));
            long cursor = log.head().seq();
            log.release(writer);
            writer = null;

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));

            assertEquals(2, frames.size());
            assertTrue(frames.get(0).event() instanceof AguiEvent.RunStarted);
            assertTrue(frames.get(1).event() instanceof AguiEvent.RunFinished);
            assertFalse(
                    frames.stream()
                            .anyMatch(
                                    frame ->
                                            frame.event() instanceof AguiEvent.TextMessageContent));
        } finally {
            if (writer != null) {
                log.release(writer);
            }
        }
    }

    @Test
    void attachReplaysTextFromCursorUntilRunEnd() throws Exception {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", Map.of("executionRunId", "exec-1"));
            append(log, writer, "message/user", message(user("u1", "hello")));
            long cursor = log.head().seq();

            Thread producer =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(50);
                                    append(
                                            log,
                                            writer,
                                            "model/chunk",
                                            modelChunk("call-1", "chat-1", textBlock("Hel")));
                                    append(
                                            log,
                                            writer,
                                            "model/chunk",
                                            modelChunk("call-1", "chat-1", textBlock("lo")));
                                    append(
                                            log,
                                            writer,
                                            "model/end",
                                            Map.of("modelCallId", "call-1", "status", "completed"));
                                    append(log, writer, "run/end", Map.of("status", "completed"));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            producer.start();

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));
            producer.join(1000);

            assertEquals(
                    1,
                    frames.stream()
                            .filter(frame -> frame.event() instanceof AguiEvent.TextMessageStart)
                            .count());
            assertEquals(
                    "Hello",
                    frames.stream()
                            .filter(frame -> frame.event() instanceof AguiEvent.TextMessageContent)
                            .map(frame -> ((AguiEvent.TextMessageContent) frame.event()).delta())
                            .reduce("", String::concat));
            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.TextMessageEnd));
            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.RunFinished));
        } finally {
            log.release(writer);
        }
    }

    @Test
    void attachReplaysToolArgumentsAndResult() throws Exception {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", Map.of("executionRunId", "exec-1"));
            long cursor = log.head().seq();

            Thread producer =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(50);
                                    append(
                                            log,
                                            writer,
                                            "model/chunk",
                                            modelChunk(
                                                    "call-1",
                                                    "chat-1",
                                                    toolUseBlock(
                                                            "tool-1",
                                                            "search",
                                                            Map.of("q", "x"),
                                                            "{\"q\":\"x\"}")));
                                    append(
                                            log,
                                            writer,
                                            "model/end",
                                            Map.of("modelCallId", "call-1", "status", "completed"));
                                    Map<String, Object> result = new LinkedHashMap<>();
                                    result.put("type", "tool_result");
                                    result.put("id", "tool-1");
                                    result.put("name", "search");
                                    result.put("output", List.of(textBlock("ok")));
                                    result.put("state", "success");
                                    append(
                                            log,
                                            writer,
                                            "action/end",
                                            Map.of("toolCallId", "tool-1", "result", result));
                                    append(log, writer, "run/end", Map.of("status", "completed"));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            producer.start();

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));
            producer.join(1000);

            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.ToolCallStart));
            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.ToolCallArgs));
            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.ToolCallEnd));
            assertTrue(
                    frames.stream()
                            .anyMatch(
                                    frame ->
                                            frame.event() instanceof AguiEvent.ToolCallResult result
                                                    && "ok".equals(result.content())));
        } finally {
            log.release(writer);
        }
    }

    @Test
    void attachDoesNotRestartToolCallAlreadyCoveredByHistory() throws Exception {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", Map.of("executionRunId", "exec-1"));
            append(
                    log,
                    writer,
                    "message/assistant",
                    message(
                            assistantToolCall(
                                    "a1",
                                    "calling",
                                    toolUse("tool-1", "search", "{\"q\":\"x\"}"))));
            long cursor = log.head().seq();

            Thread producer =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(50);
                                    append(
                                            log,
                                            writer,
                                            "tool/result",
                                            message(
                                                    toolResult(
                                                            "r1",
                                                            toolResultBlock(
                                                                    "tool-1", "search", "ok"))));
                                    append(log, writer, "run/end", Map.of("status", "completed"));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            producer.start();

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));
            producer.join(1000);

            assertFalse(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.ToolCallStart),
                    "the tool call is already part of history");
            assertTrue(
                    frames.stream()
                            .anyMatch(
                                    frame ->
                                            frame.event() instanceof AguiEvent.ToolCallResult result
                                                    && "tool-1".equals(result.toolCallId())));
        } finally {
            log.release(writer);
        }
    }

    @Test
    void attachMapsFailedRunEndToRunError() {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", Map.of("executionRunId", "exec-1"));
            long cursor = log.head().seq();
            append(log, writer, "run/end", Map.of("status", "failed", "reason", "MAX_ITERATIONS"));

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));

            assertTrue(
                    frames.stream().anyMatch(frame -> frame.event() instanceof AguiEvent.RunError));
            assertFalse(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.RunFinished));
        } finally {
            log.release(writer);
        }
    }

    @Test
    void attachRejectsCursorAheadOfHead() {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        List<AguiFramedEvent> frames =
                AguiSessionLogAttach.attach(input(), log, 10, "exec-1", CONFIG, POLL)
                        .collectList()
                        .block(Duration.ofSeconds(5));

        AguiEvent.RunError error = (AguiEvent.RunError) frames.get(frames.size() - 1).event();
        assertEquals("INVALID_INPUT_ERROR", error.code());
    }

    @Test
    void attachWithRunIdIgnoresOtherRuns() throws Exception {
        SessionLog log = AguiSessionLogTestSupport.newLog();
        SessionLog.Writer writer = writer(log);
        try {
            append(log, writer, "run/start", "exec-1", Map.of("executionRunId", "exec-1"));
            append(log, writer, "run/start", "exec-2", Map.of("executionRunId", "exec-2"));
            long cursor = log.head().seq();

            Thread producer =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(50);
                                    append(
                                            log,
                                            writer,
                                            "model/chunk",
                                            "exec-2",
                                            modelChunk("call-2", "chat-2", textBlock("other")));
                                    append(
                                            log,
                                            writer,
                                            "model/chunk",
                                            "exec-1",
                                            modelChunk("call-1", "chat-1", textBlock("mine")));
                                    append(
                                            log,
                                            writer,
                                            "run/end",
                                            "exec-2",
                                            Map.of("status", "completed"));
                                    append(
                                            log,
                                            writer,
                                            "run/end",
                                            "exec-1",
                                            Map.of("status", "completed"));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            producer.start();

            List<AguiFramedEvent> frames =
                    AguiSessionLogAttach.attach(input(), log, cursor, "exec-1", CONFIG, POLL)
                            .collectList()
                            .block(Duration.ofSeconds(5));
            producer.join(1000);

            String text =
                    frames.stream()
                            .filter(frame -> frame.event() instanceof AguiEvent.TextMessageContent)
                            .map(frame -> ((AguiEvent.TextMessageContent) frame.event()).delta())
                            .reduce("", String::concat);
            assertEquals("mine", text);
            assertTrue(
                    frames.stream()
                            .anyMatch(frame -> frame.event() instanceof AguiEvent.RunFinished));
        } finally {
            log.release(writer);
        }
    }

    private static RunAgentInput input() {
        return RunAgentInput.builder().threadId(THREAD_ID).runId("run-1").build();
    }
}
