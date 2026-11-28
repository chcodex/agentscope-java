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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.journal.JournalEvent;
import io.agentscope.core.journal.JournalProjection;
import io.agentscope.core.journal.ThreadInteractions;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.durable.DurableJson;
import io.agentscope.durable.DurableTypes.Frame;
import io.agentscope.durable.DurableTypes.Lease;
import io.agentscope.durable.DurableTypes.TurnView;
import io.agentscope.durable.HarnessContext;
import io.agentscope.durable.codec.agentscope.AgentScopeCheckpointCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BackendThreadJournalReplayTest {
    @Test
    void quietCheckpointAnchorsReplayAndSeedMatchesFullReplay() {
        var events =
                List.of(
                        event(1, "run/start", Map.of()),
                        event(2, "step/start", Map.of("stepId", "s1")),
                        event(3, "step/end", Map.of("stepId", "s1", "status", "completed")),
                        checkpoint(4, "test"));
        var context = context(events);

        var journal = new BackendThreadJournal(context, Map.of());
        assertEquals(4, journal.replayStart());
        var seed = journal.projectionSeed();
        assertEquals(4, seed.asOfSeq());
        assertTrue(seed.activeRuns().contains("run"));
        assertEquals("run", seed.positionRunId());
        assertEquals("turn", seed.positionTurnId());

        var full = JournalProjection.read(journal);
        var incremental = JournalProjection.read(journal, seed, ignored -> {});
        assertEquals(full.asOfSeq(), incremental.asOfSeq());
        assertEquals(full.stateJson(), incremental.stateJson());
        assertEquals(full.activeRuns(), incremental.activeRuns());
        assertEquals(full.uncertainToolCalls(), incremental.uncertainToolCalls());
        assertEquals(full.positionTurnId(), incremental.positionTurnId());
        assertEquals(full.positionRunId(), incremental.positionRunId());
    }

    @Test
    void pendingInteractionBlocksAQuietAnchor() {
        var context =
                context(
                        List.of(
                                event(1, "run/start", Map.of()),
                                event(
                                        2,
                                        "interaction/requested",
                                        Map.of(
                                                "requestId",
                                                "call",
                                                "kind",
                                                "external_execution",
                                                "call",
                                                Map.of(
                                                        "type",
                                                        "tool_use",
                                                        "id",
                                                        "call",
                                                        "name",
                                                        "tool",
                                                        "input",
                                                        Map.of()))),
                                checkpoint(3, "step_boundary")));

        var journal = new BackendThreadJournal(context, Map.of());
        assertEquals(0, journal.replayStart());
        assertNull(journal.projectionSeed());
        assertTrue(JournalProjection.read(journal).activeRuns().contains("run"));
        assertEquals(
                "call", ThreadInteractions.pending(journal).get("call").data().get("requestId"));
    }

    @Test
    void interruptedRunEndKeepsSeedUnfinished() {
        // An interrupted run never completed: recovery must treat it as unfinished (eligible
        // for continuation or terminal settlement), exactly like the non-seeded projection
        // does. Marking it finished here would silently skip the unfinished run on replay.
        // History mirrors the real order: a full checkpoint, then the run, then a quiet
        // periodic checkpoint that becomes the anchor, so the seed scan actually sees run/end.
        var interrupted =
                new BackendThreadJournal(
                        context(
                                List.of(
                                        event(1, "run/start", Map.of()),
                                        event(
                                                2,
                                                "run/end",
                                                Map.of(
                                                        "status",
                                                        "interrupted",
                                                        "reason",
                                                        "host lost")),
                                        checkpoint(3, "test"),
                                        event(4, "run/start", Map.of()),
                                        event(
                                                5,
                                                "run/end",
                                                Map.of(
                                                        "status",
                                                        "interrupted",
                                                        "reason",
                                                        "host lost")),
                                        checkpoint(6, "periodic"))),
                        Map.of());
        var interruptedSeed = interrupted.projectionSeed();
        assertNotNull(interruptedSeed);
        assertFalse(interruptedSeed.finished());

        // Control: a completed run is finished, so recovery does not resume it.
        var completed =
                new BackendThreadJournal(
                        context(
                                List.of(
                                        event(1, "run/start", Map.of()),
                                        event(
                                                2,
                                                "run/end",
                                                Map.of("status", "completed", "reason", "done")),
                                        checkpoint(3, "test"),
                                        event(4, "run/start", Map.of()),
                                        event(
                                                5,
                                                "run/end",
                                                Map.of("status", "completed", "reason", "done")),
                                        checkpoint(6, "periodic"))),
                        Map.of());
        var completedSeed = completed.projectionSeed();
        assertNotNull(completedSeed);
        assertTrue(completedSeed.finished());
    }

    private static HarnessContext context(List<JournalEvent> events) {
        var context = mock(HarnessContext.class);
        var frame = mock(Frame.class);
        var lease = mock(Lease.class);
        var turn = mock(TurnView.class);
        when(turn.inputMessageId()).thenReturn("input");
        when(frame.turn()).thenReturn(turn);
        when(frame.input()).thenReturn(DurableJson.value(null));
        when(frame.tools()).thenReturn(List.of());
        when(context.frame()).thenReturn(frame);
        when(context.frameWithCheckpoint()).thenReturn(frame);
        when(context.lease()).thenReturn(lease);
        when(frame.checkpoint())
                .thenReturn(
                        DurableJson.value(
                                Map.of(
                                        "format",
                                        AgentScopeCheckpointCodec.STATE_VERSION,
                                        "position",
                                        events.size())));
        when(context.journal(anyLong(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            long from = invocation.getArgument(0);
                            int limit = invocation.getArgument(1);
                            var page = new ArrayList<com.fasterxml.jackson.databind.JsonNode>();
                            for (var event : events) {
                                if (event.seq() > from && page.size() < limit)
                                    page.add(DurableJson.value(event));
                            }
                            return page;
                        });
        return context;
    }

    private static JournalEvent checkpoint(long seq, String reason) {
        String state = AgentState.builder().build().toJson();
        return event(
                seq,
                "state/checkpoint",
                Map.of(
                        "stateJson",
                        state,
                        "projectorVersion",
                        1,
                        "reason",
                        reason,
                        "checkpointId",
                        "checkpoint-" + seq));
    }

    private static JournalEvent event(long seq, String type, Map<String, Object> data) {
        return new JournalEvent(
                JournalEvent.SCHEMA_VERSION,
                "event-" + seq,
                seq,
                1,
                type,
                "run",
                "turn",
                true,
                JsonUtils.getJsonCodec().toJson(data));
    }
}
