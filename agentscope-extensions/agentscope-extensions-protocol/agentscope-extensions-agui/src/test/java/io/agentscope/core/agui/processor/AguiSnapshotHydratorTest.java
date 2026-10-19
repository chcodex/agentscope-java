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

import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.event.AguiEventType;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AguiSnapshotHydrator} frame sequencing. */
@Tag("unit")
@DisplayName("AguiSnapshotHydrator Unit Tests")
class AguiSnapshotHydratorTest {

    @Test
    @DisplayName("Frame order is RUN_STARTED, snapshots, RUN_FINISHED(interrupts?)")
    void frameOrder() {
        AguiThreadSnapshot snapshot =
                AguiThreadSnapshot.of(
                        List.of(AguiMessage.userMessage("msg-1", "hello")),
                        Map.of("cursor", 8),
                        Map.of(
                                "msg-1",
                                new AguiThreadSnapshot.ActivityEntry("map", Map.of("p", 1))));

        List<AguiEvent> events =
                AguiSnapshotHydrator.hydrateEvents("thread-1", "run-9", snapshot, List.of());

        assertEquals(
                List.of(
                        AguiEventType.RUN_STARTED,
                        AguiEventType.STATE_SNAPSHOT,
                        AguiEventType.MESSAGES_SNAPSHOT,
                        AguiEventType.ACTIVITY_SNAPSHOT,
                        AguiEventType.RUN_FINISHED),
                events.stream().map(AguiEvent::getType).toList());
    }

    @Test
    @DisplayName("Unresolved interrupts ride on the closing RUN_FINISHED")
    void unresolvedInterruptsOnRunFinished() {
        AguiEvent.Interrupt interrupt =
                new AguiEvent.Interrupt(
                        "interrupt-1", "tool_call", "approve", "call-1", null, null, null);
        AguiThreadSnapshot snapshot =
                AguiThreadSnapshot.ofMessages(List.of(AguiMessage.userMessage("msg-1", "hello")));

        List<AguiEvent> events =
                AguiSnapshotHydrator.hydrateEvents(
                        "thread-1", "run-9", snapshot, List.of(interrupt));

        AguiEvent.RunFinished finished = (AguiEvent.RunFinished) events.get(events.size() - 1);
        AguiEvent.RunFinishedInterruptOutcome outcome =
                (AguiEvent.RunFinishedInterruptOutcome) finished.outcome();
        assertEquals("interrupt-1", outcome.interrupts().get(0).id());
    }

    @Test
    @DisplayName("Empty sections are omitted; interrupt-free outcome serializes as null")
    void emptySectionsOmitted() {
        AguiThreadSnapshot snapshot = AguiThreadSnapshot.of(null, null, null);

        List<AguiEvent> events =
                AguiSnapshotHydrator.hydrateEvents("thread-1", "run-9", snapshot, List.of());

        assertEquals(
                List.of(AguiEventType.RUN_STARTED, AguiEventType.RUN_FINISHED),
                events.stream().map(AguiEvent::getType).toList());
        AguiEvent.RunFinished finished = (AguiEvent.RunFinished) events.get(1);
        assertEquals(null, finished.outcome());
    }

    @Test
    @DisplayName("Activity snapshots replay per message id in stable order")
    void activityReplaysPerMessage() {
        AguiThreadSnapshot snapshot =
                AguiThreadSnapshot.of(
                        null,
                        null,
                        Map.of(
                                "msg-b",
                                new AguiThreadSnapshot.ActivityEntry("map", Map.of("p", 2)),
                                "msg-a",
                                new AguiThreadSnapshot.ActivityEntry("map", Map.of("p", 1))));

        List<AguiEvent> events =
                AguiSnapshotHydrator.hydrateEvents("thread-1", "run-9", snapshot, List.of());

        AguiEvent.ActivitySnapshot firstActivity = (AguiEvent.ActivitySnapshot) events.get(1);
        assertEquals("msg-a", firstActivity.messageId());
        assertEquals(1, firstActivity.content().get("p"));
        AguiEvent.ActivitySnapshot secondActivity = (AguiEvent.ActivitySnapshot) events.get(2);
        assertEquals("msg-b", secondActivity.messageId());
    }
}
