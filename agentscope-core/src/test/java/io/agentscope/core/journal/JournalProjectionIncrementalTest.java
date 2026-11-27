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
package io.agentscope.core.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.agentscope.core.agent.ThreadKey;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** A seeded projection must equal a full replay over the same committed prefix. */
class JournalProjectionIncrementalTest {

    private static JournalEvent event(long seq, String type, String runId, String payload) {
        return new JournalEvent(
                JournalEvent.SCHEMA_VERSION, "e" + seq, seq, 1, type, runId, "t", true, payload);
    }

    private static String quoted(String value) {
        return JsonUtils.getJsonCodec().toJson(value);
    }

    @Test
    void seededProjectionMatchesFullReplay() {
        var log = new InMemoryThreadJournalStore().open(new ThreadKey("u", "a", "s", "main"), null);
        var writer = log.acquire("w", Duration.ofMinutes(2));
        String stateJson = AgentState.builder().build().toJson();
        log.commit(
                writer,
                "b1",
                0,
                List.of(
                        event(1, "run/start", "r1", "{}"),
                        event(
                                2,
                                "state/checkpoint",
                                "r1",
                                "{\"stateJson\":"
                                        + quoted(stateJson)
                                        + ",\"projectorVersion\":1,\"reason\":\"test\"}")));

        var base = JournalProjection.read(log);
        var seed =
                new ThreadJournal.ProjectionSeed(
                        base.asOfSeq(),
                        base.stateJson(),
                        base.uncertainToolCalls(),
                        base.activeRuns(),
                        base.positionTurnId(),
                        base.positionRunId(),
                        base.executionPosition());

        log.commit(
                writer,
                "b2",
                2,
                List.of(
                        event(3, "tool/dispatch", "r1", "{\"toolCallId\":\"call-1\"}"),
                        event(4, "run/end", "r1", "{}")));

        var full = JournalProjection.read(log);
        var incremental = JournalProjection.read(log, seed, ignored -> {});

        assertEquals(full.asOfSeq(), incremental.asOfSeq());
        assertEquals(full.stateJson(), incremental.stateJson());
        assertEquals(full.uncertainToolCalls(), incremental.uncertainToolCalls());
        assertEquals(full.activeRuns(), incremental.activeRuns());
        assertEquals(full.positionTurnId(), incremental.positionTurnId());
        assertEquals(full.positionRunId(), incremental.positionRunId());
        assertEquals(full.executionPosition(), incremental.executionPosition());
    }

    @Test
    void recorderChecksRunIdsBeforeSeedAndContinuesCheckpointChain() {
        var log =
                spy(
                        new InMemoryThreadJournalStore()
                                .open(new ThreadKey("u", "a", "s", "main"), null));
        var writer = log.acquire("w", Duration.ofMinutes(2));
        String stateJson = AgentState.builder().build().toJson();
        log.commit(
                writer,
                "batch",
                0,
                List.of(
                        event(1, "run/start", "used-run", "{}"),
                        event(
                                2,
                                "state/checkpoint",
                                "used-run",
                                "{\"stateJson\":"
                                        + quoted(stateJson)
                                        + ",\"projectorVersion\":1,\"reason\":\"test\",\"checkpointId\":\"parent\"}"),
                        event(3, "run/end", "used-run", "{\"status\":\"completed\"}")));
        log.release(writer);
        doReturn(
                        new ThreadJournal.ProjectionSeed(
                                3, stateJson, Set.of(), Set.of(), "t", "used-run", null))
                .when(log)
                .projectionSeed();

        assertThrows(
                JournalException.class, () -> new InvocationRecorder(log, "t", "used-run", null));
        var recorder = new InvocationRecorder(log, "t", "new-run", null);
        try {
            recorder.start(List.of(), AgentState.builder().build()).block(Duration.ofSeconds(5));
            var checkpoints =
                    log.readAfter(3, 100).stream()
                            .filter(event -> event.type().equals("state/checkpoint"))
                            .toList();
            assertEquals("parent", checkpoints.get(0).data().get("parentCheckpointId"));
        } finally {
            recorder.close();
        }
    }

    @Test
    void pendingInteractionsIncludeRequestsBeforeReplayStart() {
        var log =
                spy(
                        new InMemoryThreadJournalStore()
                                .open(new ThreadKey("u", "a", "s", "main"), null));
        var writer = log.acquire("w", Duration.ofMinutes(2));
        log.commit(
                writer,
                "batch",
                0,
                List.of(
                        event(
                                1,
                                "interaction/requested",
                                "run",
                                "{\"requestId\":\"call\",\"kind\":\"external_execution\"}"),
                        event(2, "run/end", "run", "{\"status\":\"suspended\"}")));
        log.release(writer);
        doReturn(2L).when(log).replayStart();
        assertEquals(
                "call",
                ThreadInteractions.continuationRequests(log, Set.of("call"))
                        .get("call")
                        .data()
                        .get("requestId"));
    }

    @Test
    void incompleteSeedCannotHideAnActiveRunOrUncertainTool() {
        var log = new InMemoryThreadJournalStore().open(new ThreadKey("u", "a", "s", "main"), null);
        var writer = log.acquire("w", Duration.ofMinutes(2));
        String state = AgentState.builder().build().toJson();
        log.commit(
                writer,
                "batch",
                0,
                List.of(
                        event(1, "run/start", "run", "{}"),
                        event(2, "tool/dispatch", "run", "{\"toolCallId\":\"call\"}"),
                        event(
                                3,
                                "state/checkpoint",
                                "run",
                                "{\"stateJson\":"
                                        + quoted(state)
                                        + ",\"projectorVersion\":1,\"reason\":\"step_boundary\"}")));
        var incomplete =
                new ThreadJournal.ProjectionSeed(3, state, Set.of(), Set.of(), "t", "run", null);
        var seen = new java.util.ArrayList<JournalEvent>();
        var result = JournalProjection.read(log, incomplete, seen::add);
        assertEquals(Set.of("run"), result.activeRuns());
        assertEquals(Set.of("call"), result.uncertainToolCalls());
        assertEquals(3, seen.size());
    }
}
