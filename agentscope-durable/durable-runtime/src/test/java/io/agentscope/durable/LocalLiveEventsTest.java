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
    void aReconnectReplaysTheDeltasOfTheCurrentStep() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));
        events.publish(live("main", "step-1", "b"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe("main", received::add)) {
            assertEquals(List.of("a", "b"), texts(received));
        }
    }

    @Test
    void aNewStepDropsThePreviousReplayWindow() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));
        events.publish(live("main", "step-2", "b"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe("main", received::add)) {
            assertEquals(List.of("b"), texts(received));
        }
    }

    @Test
    void replayWindowsAreIsolatedPerThread() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("other", "step-1", "other"));
        events.publish(live("main", "step-1", "main"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe("main", received::add)) {
            assertEquals(List.of("main"), texts(received));
        }
    }

    @Test
    void aSubscriberKeepsReceivingNewDeltasAfterTheReplay() throws Exception {
        var events = new LocalLiveEvents();
        events.publish(live("main", "step-1", "a"));

        var received = new ArrayList<LiveEvent>();
        try (var ignored = events.subscribe("main", received::add)) {
            events.publish(live("main", "step-1", "b"));
            assertEquals(List.of("a", "b"), texts(received));
        }
    }

    private static List<String> texts(List<LiveEvent> events) {
        return events.stream().map(event -> event.data().path("text").asText()).toList();
    }

    private LiveEvent live(String threadId, String stepId, String text) {
        return new LiveEvent(
                "1",
                "event-" + threadId + "-" + stepId + "-" + text,
                session,
                threadId,
                "turn-1",
                "run-1",
                stepId,
                1,
                "message.delta",
                DurableJson.value(Map.of("messageId", "message-1", "text", text)));
    }
}
