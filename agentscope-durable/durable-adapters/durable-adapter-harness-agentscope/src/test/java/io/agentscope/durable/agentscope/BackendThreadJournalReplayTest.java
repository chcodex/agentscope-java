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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BackendThreadJournalReplayTest {
    @Test
    void checkpointDoesNotHideActiveRunOrPendingInteraction() {
        var context = mock(HarnessContext.class);
        var frame = mock(Frame.class);
        var lease = mock(Lease.class);
        var turn = mock(TurnView.class);
        when(turn.inputMessageId()).thenReturn("input");
        when(frame.turn()).thenReturn(turn);
        when(frame.input()).thenReturn(DurableJson.value(null));
        when(context.frameWithCheckpoint()).thenReturn(frame);
        when(context.lease()).thenReturn(lease);
        when(frame.checkpoint())
                .thenReturn(
                        DurableJson.value(
                                Map.of(
                                        "format",
                                        AgentScopeCheckpointCodec.STATE_VERSION,
                                        "position",
                                        3)));
        String state = AgentState.builder().build().toJson();
        var events =
                List.of(
                        event(1, "run/start", Map.of()),
                        event(
                                2,
                                "interaction/requested",
                                Map.of(
                                        "requestId", "call",
                                        "kind", "external_execution",
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
                        event(
                                3,
                                "state/checkpoint",
                                Map.of(
                                        "stateJson",
                                        state,
                                        "projectorVersion",
                                        1,
                                        "reason",
                                        "step_boundary")));
        when(context.journal(0, 1000)).thenReturn(events.stream().map(DurableJson::value).toList());

        var journal = new BackendThreadJournal(context, Map.of());
        assertEquals(0, journal.replayStart());
        assertNull(journal.projectionSeed());
        assertTrue(JournalProjection.read(journal).activeRuns().contains("run"));
        assertEquals(
                "call", ThreadInteractions.pending(journal).get("call").data().get("requestId"));
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
