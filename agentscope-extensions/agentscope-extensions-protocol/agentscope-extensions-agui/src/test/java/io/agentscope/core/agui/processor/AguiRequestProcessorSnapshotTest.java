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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.agui.adapter.AguiAdapterConfig;
import io.agentscope.core.agui.adapter.AguiAgentAdapter;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.event.AguiEventType;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.model.AguiResume;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.agui.runtime.AguiRuntimeContextRequest;
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import io.agentscope.core.agui.store.InMemoryAguiSnapshotStore;
import io.agentscope.core.event.AgentEndEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Unit tests for snapshot hydration and recording wired in {@link AguiRequestProcessor}.
 */
@Tag("unit")
@DisplayName("AguiRequestProcessor Snapshot Tests")
class AguiRequestProcessorSnapshotTest {

    private static final String THREAD_ID = "thread-1";

    private static AguiRuntimeContextRequest<?> request(RunAgentInput runInput) {
        return AguiRuntimeContextRequest.builder().input(runInput).build();
    }

    private static RunAgentInput hydrateInput(String runId) {
        return RunAgentInput.builder().threadId(THREAD_ID).runId(runId).build();
    }

    private static RunAgentInput withMessages(String runId) {
        return RunAgentInput.builder()
                .threadId(THREAD_ID)
                .runId(runId)
                .messages(List.of(AguiMessage.userMessage("msg-1", "hello")))
                .build();
    }

    private static AguiEvent runStarted(RunAgentInput input) {
        return new AguiEvent.RunStarted(THREAD_ID, input.getRunId(), null, input);
    }

    private static final class ScriptedAdapter extends AguiAgentAdapter {

        private final Map<String, List<AguiEvent>> scriptsByRunId;
        private final List<AguiEvent> script;

        private ScriptedAdapter(Agent agent, AguiAdapterConfig config, List<AguiEvent> script) {
            super(agent, config);
            this.script = script;
            this.scriptsByRunId = null;
        }

        private ScriptedAdapter(
                Agent agent,
                AguiAdapterConfig config,
                Map<String, List<AguiEvent>> scriptsByRunId) {
            super(agent, config);
            this.script = null;
            this.scriptsByRunId = scriptsByRunId;
        }

        @Override
        public Flux<AguiEvent> run(RunAgentInput input, RuntimeContext runtimeContext) {
            List<AguiEvent> selected = script;
            if (scriptsByRunId != null) {
                selected = scriptsByRunId.getOrDefault(input.getRunId(), List.of());
            }
            return Flux.fromIterable(selected);
        }
    }

    private static AguiEvent.Interrupt interrupt(String id, String toolCallId) {
        return new AguiEvent.Interrupt(id, "tool_call", "approve", toolCallId, null, null, null);
    }

    @Test
    @DisplayName("Hydrate quadruple replays a read-only synthetic run without an agent")
    void hydrateQuadrupleReplaysSnapshot() {
        AgentResolver resolver = mock(AgentResolver.class);
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        store.save(
                THREAD_ID,
                AguiThreadSnapshot.of(
                        List.of(AguiMessage.userMessage("msg-1", "hello")),
                        Map.of("cursor", 8),
                        Map.of()));
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder().agentResolver(resolver).snapshotStore(store).build();

        List<AguiEvent> events =
                processor.process(request(hydrateInput("run-1"))).events().collectList().block();

        assertNotNull(events);
        assertEquals(
                List.of(
                        AguiEventType.RUN_STARTED,
                        AguiEventType.STATE_SNAPSHOT,
                        AguiEventType.MESSAGES_SNAPSHOT,
                        AguiEventType.RUN_FINISHED),
                events.stream().map(AguiEvent::getType).toList());
        AguiEvent.RunFinished finished = (AguiEvent.RunFinished) events.get(3);
        assertNull(finished.outcome());
        verify(resolver, never()).resolveAgent(any(), any(), any());
        verify(resolver, never()).hasMemory(any(RuntimeContext.class));
    }

    @Test
    @DisplayName("Missing threadId is rejected instead of falling back to a shared key")
    void missingThreadIdIsRejected() {
        AgentResolver resolver = mock(AgentResolver.class);
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder().agentResolver(resolver).snapshotStore(store).build();
        RunAgentInput blankThreadInput =
                RunAgentInput.builder().threadId("   ").runId("run-1").build();

        List<AguiEvent> events =
                processor.process(request(blankThreadInput)).events().collectList().block();

        assertEquals(
                List.of(AguiEventType.RUN_STARTED, AguiEventType.RUN_ERROR),
                events.stream().map(AguiEvent::getType).toList());
        AguiEvent.RunError error = (AguiEvent.RunError) events.get(1);
        assertEquals("threadId is required", error.message());
        verify(resolver, never()).resolveAgent(any(), any(), any());
    }

    @Test
    @DisplayName("Resume requests bypass hydration and follow the resume contract")
    void resumeRequestsBypassHydration() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        when(agent.streamEvents(anyList(), any(RuntimeContext.class)))
                .thenReturn(Flux.just(new AgentEndEvent("answer")));
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder().agentResolver(resolver).snapshotStore(store).build();
        RunAgentInput resumeInput =
                RunAgentInput.builder()
                        .threadId(THREAD_ID)
                        .runId("run-1")
                        .resume(
                                List.of(
                                        new AguiResume(
                                                "interrupt-from-server",
                                                AguiResume.STATUS_RESOLVED,
                                                Map.of("approved", true))))
                        .build();

        List<AguiEvent> events =
                processor.process(request(resumeInput)).events().collectList().block();

        // No open interrupt on the thread: contract error path, never hydration.
        assertNotNull(events);
        assertEquals(AguiEventType.RUN_STARTED, events.get(0).getType());
        assertEquals(AguiEventType.RUN_ERROR, events.get(1).getType());
        verify(resolver).resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class));
    }

    @Test
    @DisplayName("Snapshot store disabled keeps the legacy empty-input agent run")
    void storeDisabledKeepsLegacyRun() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        when(agent.streamEvents(anyList(), any(RuntimeContext.class)))
                .thenReturn(Flux.just(new AgentEndEvent("answer")));
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder().agentResolver(resolver).build();

        List<AguiEvent> events =
                processor.process(request(hydrateInput("run-1"))).events().collectList().block();

        assertNotNull(events);
        assertTrue(events.stream().noneMatch(AguiEvent.RunError.class::isInstance));
        verify(agent).streamEvents(anyList(), any(RuntimeContext.class));
    }

    @Test
    @DisplayName("Hydration decision is independent of server-side memory")
    void hydrateDecisionIndependentOfServerSideMemory() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        store.save(
                THREAD_ID,
                AguiThreadSnapshot.of(
                        List.of(AguiMessage.userMessage("msg-1", "hello")),
                        Map.of("cursor", 8),
                        Map.of()));
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder().agentResolver(resolver).snapshotStore(store).build();

        List<AguiEvent> events =
                processor.process(request(hydrateInput("run-1"))).events().collectList().block();

        assertEquals(AguiEventType.MESSAGES_SNAPSHOT, events.get(2).getType());
        verify(resolver, never()).hasMemory(any(RuntimeContext.class));
        verify(agent, never()).streamEvents(anyList(), any(RuntimeContext.class));
    }

    @Test
    @DisplayName("A completed run records a covering snapshot in the store")
    void completedRunRecordsSnapshot() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        RunAgentInput input = withMessages("run-1");
        AguiEvent scriptRunStarted = runStarted(input);
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder()
                        .agentResolver(resolver)
                        .snapshotStore(store)
                        .adapterFactory(
                                (resolvedAgent, config) ->
                                        new ScriptedAdapter(
                                                resolvedAgent,
                                                config,
                                                List.of(
                                                        scriptRunStarted,
                                                        new AguiEvent.TextMessageStart(
                                                                THREAD_ID,
                                                                "run-1",
                                                                "msg-2",
                                                                "assistant"),
                                                        new AguiEvent.TextMessageContent(
                                                                THREAD_ID, "run-1", "msg-2", "hi"),
                                                        new AguiEvent.TextMessageEnd(
                                                                THREAD_ID, "run-1", "msg-2"),
                                                        new AguiEvent.StateSnapshot(
                                                                THREAD_ID,
                                                                "run-1",
                                                                Map.of("cursor", 3)),
                                                        new AguiEvent.RunFinished(
                                                                THREAD_ID, "run-1"))))
                        .build();

        processor.process(request(input)).events().collectList().block();

        AguiThreadSnapshot snapshot = store.get(THREAD_ID);
        assertNotNull(snapshot);
        assertEquals(
                List.of(
                        AguiMessage.userMessage("msg-1", "hello"),
                        AguiMessage.assistantMessage("msg-2", "hi")),
                snapshot.messages());
        assertEquals(Map.of("cursor", 3), snapshot.state());
    }

    @Test
    @DisplayName("A failed run leaves the previous covering snapshot untouched")
    void failedRunDoesNotOverwriteSnapshot() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        AguiThreadSnapshot previous =
                AguiThreadSnapshot.ofMessages(List.of(AguiMessage.userMessage("msg-0", "old")));
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        store.save(THREAD_ID, previous);
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder()
                        .agentResolver(resolver)
                        .snapshotStore(store)
                        .adapterFactory(
                                (resolvedAgent, config) ->
                                        new ScriptedAdapter(
                                                resolvedAgent,
                                                config,
                                                List.of(
                                                        new AguiEvent.RunStarted(
                                                                THREAD_ID, "run-1", null, null),
                                                        new AguiEvent.RunError(
                                                                THREAD_ID,
                                                                "run-1",
                                                                "boom",
                                                                "INTERNAL_ERROR"))))
                        .build();

        processor.process(request(withMessages("run-1"))).events().collectList().block();

        assertEquals(previous, store.get(THREAD_ID));
    }

    @Test
    @DisplayName(
            "Interrupt run records the snapshot with pending closures; hydrate replays the"
                    + " unresolved outcome; resume run updates the transcript")
    void interruptRunSnapshotSuppressesResolvedInterrupts() {
        ReActAgent agent = mock(ReActAgent.class);
        AgentResolver resolver = mock(AgentResolver.class);
        when(resolver.resolveAgent(eq("default"), eq(THREAD_ID), nullable(String.class)))
                .thenReturn(agent);
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        RunAgentInput firstInput = withMessages("run-1");
        RunAgentInput secondInput = withMessages("run-3");
        Map<String, List<AguiEvent>> scripts = new HashMap<>();
        scripts.put(
                "run-1",
                List.of(
                        runStarted(firstInput),
                        new AguiEvent.ToolCallStart(THREAD_ID, "run-1", "call-1", "confirm"),
                        new AguiEvent.RunFinished(
                                THREAD_ID,
                                "run-1",
                                null,
                                new AguiEvent.RunFinishedInterruptOutcome(
                                        List.of(interrupt("interrupt-1", "call-1"))))));
        scripts.put(
                "run-3",
                List.of(
                        runStarted(secondInput),
                        new AguiEvent.ToolCallResult(
                                THREAD_ID, "run-3", "call-1", "approved", "tool", null),
                        new AguiEvent.RunFinished(THREAD_ID, "run-3")));
        AguiRequestProcessor processor =
                AguiRequestProcessor.builder()
                        .agentResolver(resolver)
                        .snapshotStore(store)
                        .adapterFactory(
                                (resolvedAgent, config) ->
                                        new ScriptedAdapter(resolvedAgent, config, scripts))
                        .build();

        // Run 1 ends with an unresolved interrupt: closing events keep the pending tool.
        processor.process(request(firstInput)).events().collectList().block();
        AguiThreadSnapshot interruptSnapshot = store.get(THREAD_ID);
        assertNotNull(interruptSnapshot);
        boolean hasPendingClosure =
                interruptSnapshot.messages().stream()
                        .anyMatch(
                                message ->
                                        "tool".equalsIgnoreCase(message.getRole())
                                                && "call-1".equals(message.getToolCallId())
                                                && message.getTextContent() == null);
        assertTrue(hasPendingClosure, () -> "missing dangling tool closure: " + interruptSnapshot);

        // Hydrate replays the unresolved outcome only (suppression of historical interrupts).
        List<AguiEvent> hydrateEvents =
                processor.process(request(hydrateInput("run-2"))).events().collectList().block();
        assertNotNull(hydrateEvents);
        AguiEvent.RunFinished hydrateRunFinished =
                (AguiEvent.RunFinished) hydrateEvents.get(hydrateEvents.size() - 1);
        AguiEvent.RunFinishedInterruptOutcome hydrateOutcome =
                (AguiEvent.RunFinishedInterruptOutcome) hydrateRunFinished.outcome();
        assertNotNull(hydrateOutcome);
        assertEquals("interrupt-1", hydrateOutcome.interrupts().get(0).id());

        // Run 3 resolves the interrupt through the official resume contract.
        RunAgentInput resumeInput =
                RunAgentInput.builder()
                        .threadId(THREAD_ID)
                        .runId("run-3")
                        .resume(
                                List.of(
                                        new AguiResume(
                                                "interrupt-1",
                                                AguiResume.STATUS_RESOLVED,
                                                Map.of("approved", true))))
                        .build();
        processor.process(request(resumeInput)).events().collectList().block();

        AguiThreadSnapshot resolved = store.get(THREAD_ID);
        assertNotNull(resolved);
        AguiMessage toolMessage =
                resolved.messages().stream()
                        .filter(message -> "tool".equalsIgnoreCase(message.getRole()))
                        .findFirst()
                        .orElseThrow();
        assertEquals("approved", toolMessage.getTextContent());
        List<AguiEvent> resolvedHydrate =
                processor.process(request(hydrateInput("run-4"))).events().collectList().block();
        assertNotNull(resolvedHydrate);
        AguiEvent.RunFinished hydrateResolved =
                (AguiEvent.RunFinished) resolvedHydrate.get(resolvedHydrate.size() - 1);
        assertFalse(
                hydrateResolved.outcome() instanceof AguiEvent.RunFinishedInterruptOutcome,
                () -> "resolved interrupt re-activated: " + hydrateResolved);
    }
}
