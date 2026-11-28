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
package io.agentscope.extensions.durable.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.agentscope.durable.DurableJson;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Multi-scope writes share one connection and one commit. */
class JdbcRecordStoreWriteScopesTest {
    @TempDir Path directory;

    @Test
    void multiScopeBatchCommitsOnce() {
        try (var store = JdbcRecordStore.sqlite(directory.resolve("scopes.db"))) {
            var writes =
                    new LinkedHashMap<
                            String, Map<String, com.fasterxml.jackson.databind.JsonNode>>();
            writes.put(
                    "object:a",
                    Map.of(
                            "header",
                            DurableJson.value(Map.of("key", "a")),
                            "c0",
                            DurableJson.value("x")));
            writes.put(
                    "object:b",
                    Map.of(
                            "header",
                            DurableJson.value(Map.of("key", "b")),
                            "c0",
                            DurableJson.value("y")));
            store.writeScopes(writes);
            var stored = store.readScopes(List.of("object:a", "object:b"), List.of("header", "c0"));
            assertEquals("a", stored.get("object:a").get("header").path("key").asText());
            assertEquals("x", stored.get("object:a").get("c0").asText());
            assertEquals("b", stored.get("object:b").get("header").path("key").asText());
            assertEquals("y", stored.get("object:b").get("c0").asText());
        }
    }

    @Test
    void nullPartDeletesAndBadBatchRollsBack() {
        try (var store = JdbcRecordStore.sqlite(directory.resolve("scopes-delete.db"))) {
            store.transaction(
                    "object:a",
                    tx -> {
                        tx.put("c0", DurableJson.value("x"));
                        return null;
                    });
            // A null part deletes like Transaction.put.
            var deletion = new LinkedHashMap<String, com.fasterxml.jackson.databind.JsonNode>();
            deletion.put("c0", null);
            store.writeScopes(Map.of("object:a", deletion));
            assertNull(
                    store.transaction(
                            "object:a",
                            tx -> {
                                return tx.get("c0");
                            }));
            // One bad scope key rejects the whole batch; the sibling keeps its payload.
            var writes =
                    new LinkedHashMap<
                            String, Map<String, com.fasterxml.jackson.databind.JsonNode>>();
            writes.put("object:good", Map.of("c0", DurableJson.value("kept?")));
            writes.put("x".repeat(600), Map.of("c0", DurableJson.value("bad")));
            assertThrows(IllegalArgumentException.class, () -> store.writeScopes(writes));
            assertNull(
                    store.transaction(
                            "object:good",
                            tx -> {
                                return tx.get("c0");
                            }));
        }
    }
}
