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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agui.event.AguiEvent.JsonPatchOperation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AguiJsonPatch}. */
@Tag("unit")
@DisplayName("AguiJsonPatch Unit Tests")
class AguiJsonPatchTest {

    @Test
    @DisplayName("add / replace / remove walk nested object pointers")
    void objectOperations() {
        Map<String, Object> state = Map.of("weather", Map.of("temp", 20), "cursor", 8);

        Map<String, Object> after =
                AguiJsonPatch.apply(
                        state,
                        List.of(
                                JsonPatchOperation.add("/weather/humidity", 30),
                                JsonPatchOperation.replace("/weather/temp", 25),
                                JsonPatchOperation.remove("/cursor")));

        assertEquals(25, ((Map<?, ?>) after.get("weather")).get("temp"));
        assertEquals(30, ((Map<?, ?>) after.get("weather")).get("humidity"));
        assertTrue(!after.containsKey("cursor"));
    }

    @Test
    @DisplayName("List pointers support index and '-' for array end")
    void listOperations() {
        Map<String, Object> state = Map.of("logs", List.of("a", "b"));

        Map<String, Object> after =
                AguiJsonPatch.apply(
                        state,
                        List.of(
                                JsonPatchOperation.add("/logs/0", "first"),
                                JsonPatchOperation.add("/logs/-", "last"),
                                JsonPatchOperation.replace("/logs/2", "LAST"),
                                JsonPatchOperation.remove("/logs/1")));

        assertEquals(List.of("first", "LAST", "last"), after.get("logs"));
    }

    @Test
    @DisplayName("Unresolvable pointers are skipped, baseline stays intact")
    void unresolvablePointersAreSkipped() {
        Map<String, Object> state = Map.of("a", 1);

        List<JsonPatchOperation> operations = new ArrayList<>();
        operations.add(JsonPatchOperation.remove("/missing/child"));
        operations.add(JsonPatchOperation.replace("/b/c", 2));
        operations.add(JsonPatchOperation.add("/logs/x", 3));
        operations.add(null);

        Map<String, Object> after = AguiJsonPatch.apply(state, operations);

        assertEquals(Map.of("a", 1), after);
    }

    @Test
    @DisplayName("copy/move/test and unknown operations are ignored")
    void unsupportedOperationsIgnored() {
        Map<String, Object> state = new HashMap<>();
        state.put("a", 1);

        Map<String, Object> after =
                AguiJsonPatch.apply(
                        state,
                        List.of(
                                JsonPatchOperation.add("/b", 2),
                                new JsonPatchOperation("move", "/a", null, "/b"),
                                new JsonPatchOperation("test", "/a", 1, null),
                                new JsonPatchOperation("whatever", "/c", null, null)));

        assertEquals(1, after.get("a"));
        assertEquals(2, after.get("b"));
        assertTrue(!after.containsKey("c"));
    }

    @Test
    @DisplayName("Pointer escaping follows RFC 6901 (~0 and '~1')")
    void pointerEscaping() {
        Map<String, Object> state = Map.of("a/b", 1, "a~b", 2);

        Map<String, Object> after =
                AguiJsonPatch.apply(
                        state,
                        List.of(
                                JsonPatchOperation.replace("/a~1b", 10),
                                JsonPatchOperation.replace("/a~0b", 20)));

        assertEquals(10, after.get("a/b"));
        assertEquals(20, after.get("a~b"));
    }

    @Test
    @DisplayName("Empty ops return a deep copy without mutation leaks")
    void deepCopySemantics() {
        Map<String, Object> nested = new HashMap<>();
        nested.put("v", 1);
        Map<String, Object> state = new HashMap<>();
        state.put("nested", nested);

        Map<String, Object> copy = AguiJsonPatch.apply(state, null);
        nested.put("v", 2);

        assertEquals(1, ((Map<?, ?>) copy.get("nested")).get("v"));
    }
}
