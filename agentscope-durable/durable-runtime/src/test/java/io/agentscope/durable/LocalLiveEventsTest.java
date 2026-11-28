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
package io.agentscope.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.agentscope.durable.DurableTypes.LiveEvent;
import io.agentscope.durable.DurableTypes.SessionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LocalLiveEventsTest {
    private final SessionKey session = new SessionKey("tenant", "session");

    @Test
    void aReconnectReplaysOneAccumulatedDeltaForTheCurrentStep() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));
        events.publish(live("main", "step-1", "b"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of("ab"), texts(received));
        }
    }

    @Test
    void aReconnectReceivesOneDeltaInsteadOfEveryToken() throws Exception {
        var events = new LocalLiveEvents();
        var expected = new StringBuilder();
        for (int i = 0; i < 1200; i++) {
            String text = "t" + i + ";";
            expected.append(text);
            events.publish(live("main", "step-1", text));
        }

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(1, received.size());
            assertEquals(expected.toString(), received.get(0).data().path("text").asText());
        }
    }

    @Test
    void toolOutputPreviewsKeepABoundedTailWithoutAffectingText() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "text"));
        for (int index = 0; index < 200; index++) events.publish(output("main", "step-1", index));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(
                    List.of("text"),
                    received.stream()
                            .filter(event -> "message.delta".equals(event.type()))
                            .map(event -> event.data().path("text").asText())
                            .toList());
            var outputs =
                    received.stream()
                            .filter(event -> "tool.output.delta".equals(event.type()))
                            .toList();
            assertEquals(128, outputs.size());
            assertEquals(72, outputs.get(0).data().path("offset").asInt());
            assertEquals(199, outputs.get(127).data().path("offset").asInt());
        }
    }

    @Test
    void anOversizedBlockLosesItsPreviewWithoutAffectingOtherBlocks() throws Exception {
        var events = new LocalLiveEvents();
        for (int index = 0; index < 3; index++)
            events.publish(live("main", "step-1", "x".repeat(100_000)));
        events.publish(liveBlock("main", "step-1", "block-b", "keep"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of("keep"), texts(received));
        }
    }

    @Test
    void aNewStepDropsThePreviousReplayWindow() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));
        events.publish(live("main", "step-2", "b"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of("b"), texts(received));
        }
    }

    @Test
    void replayWindowsAreIsolatedPerThread() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("other", "step-1", "other"));
        events.publish(live("main", "step-1", "main"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of("main"), texts(received));
        }
    }

    @Test
    void aSubscriberKeepsReceivingNewDeltasAfterTheReplay() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            events.publish(live("main", "step-1", "b"));
            assertEquals(List.of("a", "b"), texts(received));
        }
    }

    @Test
    void clearingAFinishedRunDropsItsReplayWindow() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));

        events.clear(session, "main");

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of(), texts(received));
        }
    }

    @Test
    void clearingOneThreadKeepsTheReplayWindowOfAnother() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "main"));
        events.publish(live("other", "step-1", "other"));

        events.clear(session, "main");

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "other", received::add)) {
            assertEquals(List.of("other"), texts(received));
        }
    }

    @Test
    void sessionsWithTheSameThreadIdDoNotSharePreviewWindows() throws Exception {
        var events = new LocalLiveEvents();
        var other = new SessionKey("other-tenant", "other-session");
        events.publish(live(other, "main", "step-1", "foreign"));
        events.publish(live("main", "step-1", "mine"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of("mine"), texts(received));
        }
    }

    @Test
    void aScopedSubscriberIgnoresLiveFramesOfAnotherSession() throws Exception {
        var events = new LocalLiveEvents();
        var other = new SessionKey("other-tenant", "other-session");
        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            events.publish(live(other, "main", "step-1", "foreign"));
            events.publish(live("main", "step-1", "mine"));
        }
        assertEquals(List.of("mine"), texts(received));
    }

    @Test
    void anOversizedPreviewIsNotKeptInTheReplayWindow() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "x".repeat(1_100_000)));
        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of(), texts(received));
        }
    }

    @Test
    void aTruncatedPreviewWindowIsNotReplayedFromTheMiddle() throws Exception {
        var events = new LocalLiveEvents();
        for (int i = 0; i < 11; i++)
            events.publish(live("main", "step-1", "x".repeat(100_000) + i));
        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe(session, "main", received::add)) {
            assertEquals(List.of(), texts(received));
        }
    }

    private static List<String> texts(List<LiveEvent> events) {
        return events.stream().map(event -> event.data().path("text").asText()).toList();
    }

    private LiveEvent live(String threadId, String stepId, String text) {
        return live(session, threadId, stepId, text);
    }

    private LiveEvent live(SessionKey eventSession, String threadId, String stepId, String text) {
        return new LiveEvent(
                "1",
                "event-" + eventSession + "-" + threadId + "-" + stepId + "-" + text.hashCode(),
                eventSession,
                threadId,
                "turn-1",
                "run-1",
                stepId,
                1,
                "message.delta",
                DurableJson.value(Map.of("messageId", "message-1", "text", text)));
    }

    private LiveEvent liveBlock(String threadId, String stepId, String blockId, String text) {
        return new LiveEvent(
                "1",
                "event-" + threadId + "-" + stepId + "-" + blockId + "-" + text.hashCode(),
                session,
                threadId,
                "turn-1",
                "run-1",
                stepId,
                1,
                "message.delta",
                DurableJson.value(
                        Map.of("messageId", "message-1", "blockId", blockId, "text", text)));
    }

    private LiveEvent output(String threadId, String stepId, int offset) {
        return new LiveEvent(
                "1",
                "output-" + threadId + "-" + offset,
                session,
                threadId,
                "turn-1",
                "run-1",
                stepId,
                1,
                "tool.output.delta",
                DurableJson.value(
                        Map.of(
                                "callId",
                                "call-1",
                                "executionId",
                                "execution-1",
                                "attempt",
                                1,
                                "revision",
                                0,
                                "offset",
                                offset,
                                "nextOffset",
                                offset + 1,
                                "data",
                                "AA==")));
    }
}
