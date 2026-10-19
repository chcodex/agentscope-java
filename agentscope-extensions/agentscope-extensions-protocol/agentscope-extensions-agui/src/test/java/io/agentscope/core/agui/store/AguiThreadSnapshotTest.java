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
package io.agentscope.core.agui.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.agentscope.core.agui.model.AguiMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AguiThreadSnapshot} deep copies. */
@Tag("unit")
@DisplayName("AguiThreadSnapshot Unit Tests")
class AguiThreadSnapshotTest {

    @Test
    @DisplayName("Snapshot messages are deep copies immune to source mutation")
    void messagesAreDeepCopied() {
        AguiMessage source = AguiMessage.userMessage("msg-1", "hello");
        List<AguiMessage> mutableMessages = new ArrayList<>();
        mutableMessages.add(source);
        AguiThreadSnapshot snapshot = AguiThreadSnapshot.ofMessages(mutableMessages);

        mutableMessages.set(0, AguiMessage.userMessage("msg-1", "mutated"));
        assertEquals("hello", snapshot.messages().get(0).getTextContent());
    }

    @Test
    @DisplayName("Snapshot state maps are deep copies")
    void stateIsDeepCopied() {
        Map<String, Object> nested = new HashMap<>();
        nested.put("cursor", 8);
        Map<String, Object> state = new HashMap<>();
        state.put("workbench", nested);
        AguiThreadSnapshot snapshot = AguiThreadSnapshot.of(null, state, null);

        nested.put("cursor", 99);
        assertEquals(8, ((Map<?, ?>) snapshot.state().get("workbench")).get("cursor"));
    }

    @Test
    @DisplayName("Activity entries copy maps and require an activity type")
    void activityEntriesAreCopied() {
        Map<String, Object> content = new HashMap<>();
        content.put("progress", 40);
        AguiThreadSnapshot.ActivityEntry entry =
                new AguiThreadSnapshot.ActivityEntry("map", content);
        content.put("progress", 100);

        assertEquals(40, entry.content().get("progress"));
        assertThrows(
                NullPointerException.class,
                () -> new AguiThreadSnapshot.ActivityEntry(null, Map.of()));
    }

    @Test
    @DisplayName("Snapshot record immutability blocks list mutation")
    void messagesListIsImmutable() {
        AguiThreadSnapshot snapshot =
                AguiThreadSnapshot.ofMessages(List.of(AguiMessage.userMessage("msg-1", "hello")));

        assertThrows(
                UnsupportedOperationException.class,
                () -> snapshot.messages().add(AguiMessage.userMessage("msg-2", "later")));
    }

    @Test
    @DisplayName("Empty sections serialize as empty collections, not null")
    void nullSectionsBecomeEmpty() {
        AguiThreadSnapshot snapshot = AguiThreadSnapshot.of(null, null, null);

        assertEquals(List.of(), snapshot.messages());
        assertEquals(Map.of(), snapshot.state());
        assertEquals(Map.of(), snapshot.activity());
    }

    @Test
    @DisplayName("Structurally-equal snapshots are equal")
    void snapshotsWithSameContentAreEqual() {
        List<AguiMessage> messages = List.of(AguiMessage.userMessage("msg-1", "hello"));
        Map<String, Object> state = Map.of("cursor", 8);
        Map<String, AguiThreadSnapshot.ActivityEntry> activity =
                Map.of("msg-1", new AguiThreadSnapshot.ActivityEntry("map", Map.of("k", "v")));

        AguiThreadSnapshot first = AguiThreadSnapshot.of(messages, state, activity);
        AguiThreadSnapshot second = AguiThreadSnapshot.of(messages, state, activity);

        assertEquals(first, second);
        assertNotEquals(first, AguiThreadSnapshot.of(messages, Map.of(), null));
    }
}
