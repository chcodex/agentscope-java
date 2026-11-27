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
package io.agentscope.durable.store;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Atomic record transactions. Implementations serialize updates across processes and supply
 * authoritative storage time. The callback is short, has no I/O, and may be retried.
 */
public interface RecordStore extends AutoCloseable {
    record Capabilities(String implementation, boolean durable, boolean sharedAcrossHosts) {}

    /** Custom stores must explicitly declare their persistence and sharing guarantees. */
    default Capabilities capabilities() {
        return new Capabilities("custom", false, false);
    }

    record Record(JsonNode value, long now) {}

    record Update<T>(JsonNode value, T result) {}

    /** A transaction owns one scope. All parts and queue changes commit atomically with it. */
    interface Transaction {
        long now();

        JsonNode get(String part);

        /**
         * Reads several parts in one round trip when the store supports it.
         *
         * <p>Ordered journal recovery reads thousands of consecutive parts; doing that one
         * {@link #get(String)} per part costs one network round trip each on a remote database.
         * The default keeps per-part semantics so existing stores stay correct.
         *
         * @return present values keyed by part; absent parts are omitted
         */
        default Map<String, JsonNode> getAll(List<String> parts) {
            if (parts == null || parts.isEmpty()) return Map.of();
            var result = new java.util.LinkedHashMap<String, JsonNode>();
            for (String part : parts) {
                JsonNode value = get(part);
                if (value != null) result.put(part, value);
            }
            return result;
        }

        void put(String part, JsonNode value);

        List<String> keys(String prefix, String after, int limit);

        /**
         * Atomically schedules one item within this transaction's scope. Different items in the
         * same scope and queue remain independent. A negative time removes only this item.
         */
        void schedule(String queue, String workId, long readyAt, JsonNode value);
    }

    record Work(String scope, String workId, long readyAt, JsonNode value) {}

    <T> T transaction(String scope, Function<Transaction, T> action);

    default <T> T transact(String key, Function<Record, Update<T>> action) {
        return transaction(
                key,
                tx -> {
                    var update = action.apply(new Record(tx.get(""), tx.now()));
                    tx.put("", update.value());
                    return update.result();
                });
    }

    /** Indexed, bounded discovery using authoritative store time; does not grant ownership. */
    List<Work> ready(String queue, int limit);

    /** Optional scoped discovery for change hints. Periodic discovery still uses the global index. */
    default List<Work> ready(String queue, String scope, int limit) {
        return List.of();
    }

    /** Stable key pagination; discovery never claims execution ownership. */
    List<String> keys(String prefix, String after, int limit);

    @Override
    default void close() {}
}
