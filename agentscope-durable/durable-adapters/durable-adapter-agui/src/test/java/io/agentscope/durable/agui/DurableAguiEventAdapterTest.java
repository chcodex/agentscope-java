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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agui.AguiInterruptConstants;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.durable.DurableJson;
import io.agentscope.durable.DurableTypes.PublicEvent;
import io.agentscope.durable.DurableTypes.SessionKey;
import io.agentscope.durable.DurableTypes.ToolOutputDelta;
import io.agentscope.durable.EventStreamOptions.EventDeltaType;
import io.agentscope.durable.SessionStreamEvent;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DurableAguiEventAdapterTest {
    private final SessionKey session = new SessionKey("tenant", "session");
    private final DurableAguiEventAdapter adapter = new DurableAguiEventAdapter();

    @Test
    void liveTextAndCommittedMessageFormOneAguiMessageLifecycle() {
        var start = adapter.map(started("message-1"));
        var delta = adapter.map(delta("message-1", "hello"));
        var completed =
                adapter.map(
                        new SessionStreamEvent.Committed(completed("message-1", "hello world")));

        assertInstanceOf(AguiEvent.TextMessageStart.class, start.get(0));
        assertEquals("hello", ((AguiEvent.TextMessageContent) delta.get(0)).delta());
        assertEquals(2, completed.size());
        assertEquals(" world", ((AguiEvent.TextMessageContent) completed.get(0)).delta());
        assertInstanceOf(AguiEvent.TextMessageEnd.class, completed.get(1));
    }

    @Test
    void committedMessageWithoutLivePreviewStillFormsACompleteLifecycle() {
        var events = adapter.committed(completed("message-2", "complete"));
        assertInstanceOf(AguiEvent.TextMessageStart.class, events.get(0));
        assertEquals("complete", ((AguiEvent.TextMessageContent) events.get(1)).delta());
        assertInstanceOf(AguiEvent.TextMessageEnd.class, events.get(2));
    }

    @Test
    void committedResultApplicationDoesNotRepeatTheToolResult() {
        var applied =
                new PublicEvent(
                        "1",
                        "applied",
                        session,
                        "main",
                        "turn",
                        "run",
                        "step",
                        "2",
                        "cursor",
                        0,
                        "tool.call.updated",
                        DurableJson.value(
                                Map.of("callId", "tool", "status", "SUCCEEDED", "applied", true)));
        assertEquals(List.of(), adapter.committed(applied));
    }

    @Test
    void toolOutputPreviewDoesNotBecomeAnAguiToolResult() {
        var data = new ToolOutputDelta("call", "execution", 1, 0, 0, 3, "YWJj");
        var delta =
                new SessionStreamEvent.EventDelta(
                        "call",
                        EventDeltaType.TOOL_OUTPUT,
                        null,
                        "main",
                        "turn",
                        "run",
                        "step",
                        DurableJson.value(data));
        var preview = assertInstanceOf(AguiEvent.Custom.class, adapter.map(delta).get(0));
        assertEquals("agentscope.tool_output_delta", preview.name());
        assertEquals(DurableJson.value(data), DurableJson.value(preview.value()));
    }

    @Test
    void extensionEventIsForwardedAsCustomFrame() {
        var events =
                adapter.committed(
                        committed(
                                "x.result_metadata",
                                "run",
                                null,
                                Map.of(
                                        "deliver_artifacts",
                                        List.of(Map.of("url", "https://files/report.pdf")))));

        assertEquals(1, events.size());
        var custom = assertInstanceOf(AguiEvent.Custom.class, events.get(0));
        assertEquals("x.result_metadata", custom.name());
        assertEquals("run", custom.runId());
        assertEquals(
                List.of(Map.of("url", "https://files/report.pdf")),
                ((Map<?, ?>) custom.value()).get("deliver_artifacts"));
    }

    @Test
    void committedTurnWaitingFinishesTheRunWithEveryPendingInteraction() {
        String confirmId = DurableJson.hash(List.of("turn", "call-1"));
        String toolId = DurableJson.hash(List.of("turn", "call-2"));
        var events =
                adapter.map(
                        waiting(
                                "run",
                                List.of(
                                        interaction(confirmId, "call-1", "confirmation", false),
                                        interaction(
                                                toolId, "call-2", "external_execution", true))));

        assertEquals(1, events.size());
        var finished = assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
        assertEquals("run", finished.runId());
        var outcome =
                assertInstanceOf(AguiEvent.RunFinishedInterruptOutcome.class, finished.outcome());
        assertEquals(2, outcome.interrupts().size());

        var confirmation = outcome.interrupts().get(0);
        assertEquals(confirmId, confirmation.id());
        assertEquals(AguiInterruptConstants.TOOL_CALL_INTERRUPT_REASON, confirmation.reason());
        assertEquals("call-1", confirmation.toolCallId());
        assertEquals(
                "Tool 'send_email' requires user confirmation before execution",
                confirmation.message());
        assertEquals(Map.of("type", "boolean"), confirmation.responseSchema());
        assertEquals(
                "send_email",
                confirmation.metadata().get(AguiInterruptConstants.METADATA_TOOL_NAME));
        assertEquals(
                AguiInterruptConstants.INTERRUPT_KIND_PERMISSION_CONFIRM,
                confirmation
                        .metadata()
                        .get(AguiInterruptConstants.METADATA_AGENTSCOPE_INTERRUPT_KIND));
        assertEquals(
                Map.of("to", "person@example.com"),
                confirmation.metadata().get(AguiInterruptConstants.METADATA_TOOL_INPUT));
        assertEquals(false, confirmation.metadata().get("automatic"));

        var external = outcome.interrupts().get(1);
        assertEquals(toolId, external.id());
        assertEquals("call-2", external.toolCallId());
        assertEquals("Tool 'search' requires external execution", external.message());
        assertEquals(true, external.metadata().get("automatic"));
        assertEquals(
                "external_execution",
                external.metadata().get(AguiInterruptConstants.METADATA_AGENTSCOPE_INTERRUPT_KIND));
    }

    @Test
    void turnWaitingClosesOpenStepsMessagesAndToolCallsBeforeTheRunFinishes() {
        adapter.map(started("message-1"));
        adapter.committed(committed("step.started", "run", "step-1", Map.of()));
        adapter.map(toolDelta("call-9", "search", "{\"q\":"));

        var events =
                adapter.map(
                        waiting(
                                "run",
                                List.of(
                                        interaction(
                                                "interrupt-1", "call-1", "confirmation", false))));

        assertEquals(4, events.size());
        // Messages and tool calls belong to the Step and must end before STEP_FINISHED.
        assertEquals("message-1", ((AguiEvent.TextMessageEnd) events.get(0)).messageId());
        assertEquals("call-9", ((AguiEvent.ToolCallEnd) events.get(1)).toolCallId());
        assertEquals("step-1", ((AguiEvent.StepFinished) events.get(2)).stepName());
        assertInstanceOf(AguiEvent.RunFinished.class, events.get(3));
    }

    @Test
    void turnWaitingWithoutPendingInteractionsDoesNotFinishTheRun() {
        assertTrue(adapter.map(waiting("run", List.of())).isEmpty());
        assertTrue(adapter.map(waiting(null, List.of(Map.of("id", "interrupt-1")))).isEmpty());
    }

    @Test
    void repeatedTurnWaitingForTheSameRunFinishesItOnce() {
        var waiting =
                waiting(
                        "run",
                        List.of(interaction("interrupt-1", "call-1", "confirmation", false)));

        assertEquals(1, adapter.map(waiting).size());
        assertTrue(adapter.map(waiting).isEmpty());
    }

    @Test
    void suspendedRunEndWaitsForTheTurnWaitingEvent() {
        var ended =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "STOPPED", "reason", "suspended")));
        assertTrue(ended.isEmpty());

        var events =
                adapter.map(
                        waiting(
                                "run",
                                List.of(
                                        interaction(
                                                "interrupt-1", "call-1", "confirmation", false))));
        assertEquals(1, events.size());
        assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
    }

    @Test
    void failureAfterAnInterruptedRunIsReportedAsALateRunError() {
        adapter.map(
                waiting(
                        "run",
                        List.of(interaction("interrupt-1", "call-1", "confirmation", false))));

        var events =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "FAILED", "reason", "boom")));

        assertEquals(1, events.size());
        assertInstanceOf(AguiEvent.RunError.class, events.get(0));
    }

    @Test
    void turnWaitingAfterAFailedRunDoesNotFinishItAgain() {
        adapter.committed(
                committed("run.ended", "run", null, Map.of("status", "FAILED", "reason", "boom")));

        assertTrue(
                adapter.map(
                                waiting(
                                        "run",
                                        List.of(
                                                interaction(
                                                        "interrupt-1",
                                                        "call-1",
                                                        "confirmation",
                                                        false))))
                        .isEmpty());
    }

    @Test
    void failedRunClosesOpenStepsBeforeTheRunError() {
        adapter.committed(committed("step.started", "run", "step-1", Map.of()));

        var events =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "FAILED", "reason", "boom")));

        assertEquals(2, events.size());
        assertEquals("step-1", ((AguiEvent.StepFinished) events.get(0)).stepName());
        assertInstanceOf(AguiEvent.RunError.class, events.get(1));
    }

    @Test
    void permissionAskingRunEndWaitsForTheTurnWaitingEvent() {
        assertParkedRunEndWaitsForTurnWaiting("PERMISSION_ASKING");
    }

    @Test
    void toolSuspendedRunEndWaitsForTheTurnWaitingEvent() {
        assertParkedRunEndWaitsForTurnWaiting("TOOL_SUSPENDED");
    }

    private void assertParkedRunEndWaitsForTurnWaiting(String reason) {
        var ended =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "STOPPED", "reason", reason)));
        assertTrue(ended.isEmpty());

        var events =
                adapter.map(
                        waiting(
                                "run",
                                List.of(
                                        interaction(
                                                "interrupt-1", "call-1", "confirmation", false))));

        assertEquals(1, events.size());
        var finished = assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
        var outcome =
                assertInstanceOf(AguiEvent.RunFinishedInterruptOutcome.class, finished.outcome());
        assertEquals(1, outcome.interrupts().size());
        assertEquals("interrupt-1", outcome.interrupts().get(0).id());
    }

    @Test
    void runStoppedWithoutWaitingFinishesAsCancelled() {
        var events =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "STOPPED", "reason", "host_released")));

        assertEquals(1, events.size());
        var finished = assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
        assertInstanceOf(AguiEvent.RunFinishedCancelledOutcome.class, finished.outcome());
    }

    @Test
    void hostPausedRunEndFinishesAsCancelled() {
        var events =
                adapter.committed(
                        committed(
                                "run.ended",
                                "run",
                                null,
                                Map.of("status", "STOPPED", "reason", "HOST_PAUSED")));

        assertEquals(1, events.size());
        var finished = assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
        assertInstanceOf(AguiEvent.RunFinishedCancelledOutcome.class, finished.outcome());
    }

    @Test
    void completedRunFinishesWithTheSuccessOutcome() {
        var events =
                adapter.committed(
                        committed("run.ended", "run", null, Map.of("status", "COMPLETED")));

        var finished = assertInstanceOf(AguiEvent.RunFinished.class, events.get(0));
        assertInstanceOf(AguiEvent.RunFinishedSuccessOutcome.class, finished.outcome());
    }

    @Test
    void toolResultCarriesItsOwnMessageId() {
        var events =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId",
                                        "call-1",
                                        "status",
                                        "SUCCEEDED",
                                        "content",
                                        "done")));

        var result = assertInstanceOf(AguiEvent.ToolCallResult.class, events.get(0));
        assertEquals("call-1:result", result.messageId());
    }

    private SessionStreamEvent waiting(String runId, List<Map<String, Object>> interactions) {
        return new SessionStreamEvent.Committed(
                committed(
                        "turn.waiting",
                        runId,
                        null,
                        Map.of("revision", 1, "attempt", 1, "interactions", interactions)));
    }

    private Map<String, Object> interaction(
            String id, String requestId, String kind, boolean automatic) {
        return Map.of(
                "id",
                id,
                "requestId",
                requestId,
                "turnId",
                "turn",
                "key",
                "interaction:" + requestId,
                "kind",
                kind,
                "status",
                "pending",
                "call",
                Map.of(
                        "name",
                        "confirmation".equals(kind) ? "send_email" : "search",
                        "arguments",
                        Map.of("to", "person@example.com")),
                "automatic",
                automatic);
    }

    private PublicEvent committed(
            String type, String runId, String stepId, Map<String, Object> data) {
        return new PublicEvent(
                "1",
                "event",
                session,
                "main",
                "turn",
                runId,
                stepId,
                "2",
                "cursor",
                0,
                type,
                DurableJson.value(data));
    }

    private SessionStreamEvent.PreviewStarted started(String id) {
        return new SessionStreamEvent.PreviewStarted(
                id,
                EventDeltaType.MESSAGE,
                "main",
                "turn",
                "run",
                "step",
                DurableJson.value(Map.of("messageId", id)));
    }

    private SessionStreamEvent.EventDelta delta(String id, String text) {
        return new SessionStreamEvent.EventDelta(
                id,
                EventDeltaType.MESSAGE,
                "text-1",
                "main",
                "turn",
                "run",
                "step",
                DurableJson.value(Map.of("messageId", id, "text", text)));
    }

    private SessionStreamEvent.EventDelta liveTwin(String nativeId, String id, String text) {
        return new SessionStreamEvent.EventDelta(
                id,
                EventDeltaType.MESSAGE,
                "text-1",
                "main",
                "turn",
                "run",
                "step",
                DurableJson.value(Map.of("messageId", id, "text", text)),
                nativeId);
    }

    private PublicEvent committedDelta(String nativeId, String id, String text) {
        return new PublicEvent(
                "1",
                nativeId,
                session,
                "main",
                "turn",
                "run",
                "step",
                "1",
                "cursor-" + nativeId,
                0,
                "message.delta",
                DurableJson.value(
                        Map.of("messageId", id, "attempt", 1, "revision", 0, "text", text)));
    }

    private SessionStreamEvent.EventDelta toolDelta(String id, String name, String text) {
        return new SessionStreamEvent.EventDelta(
                id,
                EventDeltaType.TOOL_ARGUMENTS,
                id,
                "main",
                "turn",
                "run",
                "step",
                DurableJson.value(Map.of("toolCallId", id, "name", name, "text", text)));
    }

    @Test
    void lostRunClosesTheStreamWithRunError() {
        var lost =
                committed(
                        "run.lost", "run-1", null, Map.of("status", "LOST", "reason", "host lost"));
        var events = adapter.committed(lost);
        assertTrue(events.stream().anyMatch(e -> e instanceof AguiEvent.RunError));
    }

    @Test
    void placeholderToolNameDefersStartUntilCommittedRequest() {
        var live = adapter.map(toolDelta("call-1", "__fragment__", "{\"comm"));
        assertTrue(live.isEmpty());

        var requested =
                committed(
                        "tool.call.updated",
                        "run",
                        "step",
                        Map.of(
                                "callId", "call-1",
                                "status", "REQUESTED",
                                "name", "execute",
                                "arguments", Map.of("command", "ls")));
        var events = adapter.committed(requested);
        assertEquals(3, events.size());
        var start = assertInstanceOf(AguiEvent.ToolCallStart.class, events.get(0));
        assertEquals("execute", start.toolCallName());
    }

    @Test
    void liveToolArgumentsAreNotDuplicatedByTheCommittedRequest() {
        // Live preview streams the arguments fragment by fragment.
        var live = adapter.map(toolDelta("call-1", "execute", "{\"command\":\"ls\"}"));
        assertEquals(2, live.size());
        assertInstanceOf(AguiEvent.ToolCallStart.class, live.get(0));
        assertInstanceOf(AguiEvent.ToolCallArgs.class, live.get(1));

        // The committed request repeats the complete arguments; re-sending them would make an
        // append-style client concatenate two JSON objects. Only the end frame remains.
        var committedRequest =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId", "call-1",
                                        "status", "REQUESTED",
                                        "name", "execute",
                                        "arguments", Map.of("command", "ls"))));
        assertEquals(1, committedRequest.size());
        var end = assertInstanceOf(AguiEvent.ToolCallEnd.class, committedRequest.get(0));
        assertEquals("call-1", end.toolCallId());
    }

    @Test
    void truncatedLiveArgumentsAreCompletedByTheCommittedSuffix() {
        var live = adapter.map(toolDelta("call-1", "execute", "{\"command\":"));
        assertEquals(2, live.size());

        var committedRequest =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId", "call-1",
                                        "status", "REQUESTED",
                                        "name", "execute",
                                        "arguments", Map.of("command", "ls"))));
        assertEquals(2, committedRequest.size());
        var args = assertInstanceOf(AguiEvent.ToolCallArgs.class, committedRequest.get(0));
        assertEquals("\"ls\"}", args.delta());
        assertInstanceOf(AguiEvent.ToolCallEnd.class, committedRequest.get(1));
    }

    @Test
    void whitespaceOnlyPreviewDifferenceDoesNotDuplicateCommittedArguments() {
        var live = adapter.map(toolDelta("call-1", "execute", "{\"command\": \"ls\"}"));
        assertEquals(2, live.size());

        var committedRequest =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId", "call-1",
                                        "status", "REQUESTED",
                                        "name", "execute",
                                        "arguments", Map.of("command", "ls"))));
        assertEquals(1, committedRequest.size());
        assertInstanceOf(AguiEvent.ToolCallEnd.class, committedRequest.get(0));
    }

    @Test
    void reconnectedCommittedRequestStillCarriesItsArguments() {
        // A fresh adapter has seen no live preview: the committed request is the only source.
        var events =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId", "call-2",
                                        "status", "REQUESTED",
                                        "name", "execute",
                                        "arguments", Map.of("command", "pwd"))));
        assertEquals(3, events.size());
        assertInstanceOf(AguiEvent.ToolCallStart.class, events.get(0));
        var args = assertInstanceOf(AguiEvent.ToolCallArgs.class, events.get(1));
        assertEquals("{\"command\":\"pwd\"}", args.delta());
    }

    @Test
    void terminalToolResultIsDeliveredOnceWithItsRealContent() {
        var first =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId",
                                        "call-3",
                                        "status",
                                        "SUCCEEDED",
                                        "content",
                                        List.of(
                                                Map.of(
                                                        "type",
                                                        "text",
                                                        "text",
                                                        "Exit code: 0\n\nhello-world")))));
        var result = assertInstanceOf(AguiEvent.ToolCallResult.class, first.get(0));
        assertTrue(result.content().contains("Exit code: 0"));
        assertTrue(result.content().contains("hello-world"));

        // A later placeholder update carries no content; it must not replace the real output.
        var placeholder =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of("callId", "call-3", "status", "SUCCEEDED")));
        assertTrue(placeholder.isEmpty());
    }

    @Test
    void placeholderTerminalUpdateIsReplacedByRealContentButNeverTheReverse() {
        var placeholder =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of("callId", "call-4", "status", "SUCCEEDED")));
        var fallback = assertInstanceOf(AguiEvent.ToolCallResult.class, placeholder.get(0));
        assertEquals("SUCCEEDED", fallback.content());

        var real =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId",
                                        "call-4",
                                        "status",
                                        "SUCCEEDED",
                                        "content",
                                        List.of(Map.of("type", "text", "text", "42")))));
        var repaired = assertInstanceOf(AguiEvent.ToolCallResult.class, real.get(0));
        assertTrue(repaired.content().contains("42"));

        var again =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of("callId", "call-4", "status", "SUCCEEDED")));
        assertTrue(again.isEmpty());
    }

    @Test
    void runningUpdateProducesNoAguiResult() {
        // Effective state is resolved where the event is produced, so a RUNNING update here means
        // the call really is still in flight and must not surface as a tool result yet.
        var running =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId",
                                        "call-5",
                                        "status",
                                        "RUNNING",
                                        "content",
                                        List.of(Map.of("type", "text", "text", "partial")))));
        assertTrue(running.isEmpty());

        var terminal =
                adapter.committed(
                        committed(
                                "tool.call.updated",
                                "run",
                                "step-1",
                                Map.of(
                                        "callId",
                                        "call-5",
                                        "status",
                                        "SUCCEEDED",
                                        "content",
                                        List.of(Map.of("type", "text", "text", "final-result")))));
        var result = assertInstanceOf(AguiEvent.ToolCallResult.class, terminal.get(0));
        assertTrue(result.content().contains("final-result"), result.content());
    }

    @Test
    void reasoningResumesWithAStartAfterBeingClosedByVisibleOutput() {
        Map<String, Object> thinking =
                Map.of("messageId", "thinking-1", "reasoning", true, "text", "first");
        var started = adapter.map((SessionStreamEvent) deltaEvent(thinking));
        assertEquals(2, started.size());
        assertInstanceOf(AguiEvent.ReasoningMessageStart.class, started.get(0));
        assertInstanceOf(AguiEvent.ReasoningMessageContent.class, started.get(1));

        // Visible text closes the reasoning message (END).
        var visible =
                adapter.map(
                        (SessionStreamEvent)
                                deltaEvent(Map.of("messageId", "text-1", "text", "answer")));
        assertTrue(
                visible.stream().anyMatch(AguiEvent.ReasoningMessageEnd.class::isInstance),
                "visible output must close the open reasoning message: " + visible);

        // The model keeps thinking under the same id: the client needs a fresh START first.
        var resumed =
                adapter.map(
                        (SessionStreamEvent)
                                deltaEvent(
                                        Map.of(
                                                "messageId",
                                                "thinking-1",
                                                "reasoning",
                                                true,
                                                "text",
                                                "second")));
        assertEquals(2, resumed.size());
        assertInstanceOf(AguiEvent.ReasoningMessageStart.class, resumed.get(0));
        assertInstanceOf(AguiEvent.ReasoningMessageContent.class, resumed.get(1));
    }

    private SessionStreamEvent deltaEvent(Map<String, Object> data) {
        return new SessionStreamEvent.EventDelta(
                String.valueOf(data.get("messageId")),
                EventDeltaType.MESSAGE,
                "text-1",
                "main",
                "turn",
                "run",
                "step",
                DurableJson.value(data));
    }

    @Test
    void replayedStepEndWithoutStartIsPaired() {
        var end = committed("step.ended", "run", "step-1", Map.of());
        var events = adapter.committed(end);
        assertEquals(2, events.size());
        assertInstanceOf(AguiEvent.StepStarted.class, events.get(0));
        assertInstanceOf(AguiEvent.StepFinished.class, events.get(1));
    }

    @Test
    void reasoningDeltasMapToReasoningEventsAndCloseBeforeVisibleText() {
        var reasoning =
                committed(
                        "message.delta",
                        "run",
                        "step",
                        Map.of(
                                "messageId",
                                "reasoning-1",
                                "attempt",
                                1,
                                "revision",
                                0,
                                "text",
                                "thinking",
                                "reasoning",
                                true));
        var reasoningEvents = adapter.committed(reasoning);
        assertEquals(2, reasoningEvents.size());
        assertInstanceOf(AguiEvent.ReasoningMessageStart.class, reasoningEvents.get(0));
        var chunk =
                assertInstanceOf(AguiEvent.ReasoningMessageContent.class, reasoningEvents.get(1));
        assertEquals("thinking", chunk.delta());

        var visible = adapter.committed(committedDelta("native-visible", "message-1", "hello"));
        assertEquals(3, visible.size());
        assertInstanceOf(AguiEvent.ReasoningMessageEnd.class, visible.get(0));
        assertInstanceOf(AguiEvent.TextMessageStart.class, visible.get(1));
        assertInstanceOf(AguiEvent.TextMessageContent.class, visible.get(2));
    }

    @Test
    void committedDeltasReplayOriginalChunks() {
        var first = adapter.committed(committedDelta("native-1", "message-1", "he"));
        var second = adapter.committed(committedDelta("native-2", "message-1", "llo"));
        var done = adapter.committed(completed("message-1", "hello"));

        assertInstanceOf(AguiEvent.TextMessageStart.class, first.get(0));
        assertEquals("he", ((AguiEvent.TextMessageContent) first.get(1)).delta());
        assertEquals(1, second.size());
        assertEquals("llo", ((AguiEvent.TextMessageContent) second.get(0)).delta());
        // The completed frame only closes: the text was already delivered as deltas.
        assertEquals(1, done.size());
        assertInstanceOf(AguiEvent.TextMessageEnd.class, done.get(0));
    }

    @Test
    void liveThenCommittedTwinIsDeliveredOnce() {
        var live = adapter.map(liveTwin("native-1", "message-1", "he"));
        var committed = adapter.committed(committedDelta("native-1", "message-1", "he"));

        assertEquals(2, live.size());
        assertEquals("he", ((AguiEvent.TextMessageContent) live.get(1)).delta());
        assertTrue(committed.isEmpty(), "the committed twin must not repeat live content");
    }

    @Test
    void committedThenLiveTwinIsDeliveredOnce() {
        var committed = adapter.committed(committedDelta("native-1", "message-1", "he"));
        var live = adapter.map(liveTwin("native-1", "message-1", "he"));

        assertEquals(2, committed.size());
        assertTrue(live.isEmpty(), "the live twin must not repeat replayed content");
    }

    @Test
    void replayEqualsLiveForTheSameNativeSequence() {
        var liveAdapter = new DurableAguiEventAdapter();
        var liveFrames = new java.util.ArrayList<AguiEvent>();
        liveFrames.addAll(liveAdapter.map(liveTwin("native-1", "message-1", "he")));
        liveFrames.addAll(liveAdapter.map(liveTwin("native-2", "message-1", "llo")));
        liveFrames.addAll(liveAdapter.committed(completed("message-1", "hello")));

        var replayAdapter = new DurableAguiEventAdapter();
        var replayed = new java.util.ArrayList<AguiEvent>();
        replayed.addAll(replayAdapter.committed(committedDelta("native-1", "message-1", "he")));
        replayed.addAll(replayAdapter.committed(committedDelta("native-2", "message-1", "llo")));
        replayed.addAll(replayAdapter.committed(completed("message-1", "hello")));

        assertEquals(
                liveFrames.stream().map(Object::toString).toList(),
                replayed.stream().map(Object::toString).toList());
    }

    @Test
    void aToolOnlyAssistantMessageDoesNotOpenAnEmptyTextMessage() {
        var event =
                new PublicEvent(
                        "1",
                        "event",
                        session,
                        "main",
                        "turn",
                        "run",
                        "step",
                        "1",
                        "cursor",
                        0,
                        "message.completed",
                        DurableJson.value(
                                Map.of(
                                        "messageId",
                                        "message-tool",
                                        "attempt",
                                        1,
                                        "revision",
                                        0,
                                        "text",
                                        "",
                                        "message",
                                        Map.of(
                                                "schemaVersion",
                                                "1",
                                                "messageId",
                                                "message-tool",
                                                "role",
                                                "assistant",
                                                "content",
                                                List.of(
                                                        Map.of(
                                                                "type",
                                                                "tool_use",
                                                                "id",
                                                                "call-1",
                                                                "name",
                                                                "calculate",
                                                                "input",
                                                                Map.of()))))));
        assertTrue(adapter.committed(event).isEmpty());
    }

    private PublicEvent completed(String id, String text) {
        return new PublicEvent(
                "1",
                "event",
                session,
                "main",
                "turn",
                "run",
                "step",
                "1",
                "cursor",
                1,
                "message.completed",
                DurableJson.value(
                        Map.of(
                                "messageId",
                                id,
                                "attempt",
                                1,
                                "revision",
                                0,
                                "text",
                                text,
                                "message",
                                Map.of(
                                        "schemaVersion",
                                        "1",
                                        "messageId",
                                        id,
                                        "role",
                                        "assistant",
                                        "content",
                                        List.of(Map.of("type", "text", "text", text))))));
    }
}
