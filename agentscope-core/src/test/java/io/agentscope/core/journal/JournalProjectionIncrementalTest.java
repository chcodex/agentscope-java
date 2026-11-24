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

import io.agentscope.core.agent.ThreadKey;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import java.time.Duration;
import java.util.List;
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
}
