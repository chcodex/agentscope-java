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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

        /**
         * Reads the latest committed value for a part after this transaction holds the scope lock.
         * The default uses {@link #get(String)}; stores with snapshot isolation override it with a
         * locking read so validation sees rows committed after the snapshot was taken.
         */
        default JsonNode getLatest(String part) {
            return get(part);
        }

        List<String> keys(String prefix, String after, int limit);

        /**
         * Atomically schedules one item within this transaction's scope. Different items in the
         * same scope and queue remain independent. A negative time removes only this item.
         */
        void schedule(String queue, String workId, long readyAt, JsonNode value);
    }

    record Work(String scope, String workId, long readyAt, JsonNode value) {}

    <T> T transaction(String scope, Function<Transaction, T> action);

    /**
     * Read-only variant of {@link #transaction}: implementations may use a consistent snapshot
     * without taking the scope's write lock. The action must not write; writes are discarded by
     * stores that enforce read-only execution.
     *
     * <p>The default keeps the transactional contract so existing stores stay correct.
     */
    default <T> T readTransaction(String scope, Function<Transaction, T> action) {
        return transaction(scope, action);
    }

    /**
     * Transaction that may publish append-only parts before taking the scope lock. Implementations
     * that can do so acquire the scope lock only after {@code action} and the early-part flush, then
     * run {@code afterLock} to revalidate the snapshot and save the remaining state.
     *
     * <p>The default keeps the lock-first contract and runs {@code afterLock} inside the same
     * transaction; existing stores remain correct.
     */
    default <T> T transactionDeferredLock(
            String scope,
            Function<Transaction, T> action,
            Predicate<String> earlyPart,
            Consumer<Transaction> afterLock) {
        return transaction(
                scope,
                tx -> {
                    T result = action.apply(tx);
                    afterLock.accept(tx);
                    return result;
                });
    }

    /**
     * Reads the same part set from several independent scopes in one round trip when the store
     * supports it. Externalized objects live in their own scope each, so a long replay would
     * otherwise pay one transaction per object.
     */
    default Map<String, Map<String, JsonNode>> readScopes(List<String> scopes, List<String> parts) {
        var result = new java.util.LinkedHashMap<String, Map<String, JsonNode>>(scopes.size());
        for (var scope : scopes) {
            result.put(
                    scope,
                    transaction(
                            scope,
                            tx -> {
                                var values = new java.util.LinkedHashMap<String, JsonNode>();
                                // One batch per scope instead of one query per part: a 64 MiB
                                // object has 64 chunks, and probing them one by one dominates
                                // replay.
                                for (var entry : tx.getAll(parts).entrySet())
                                    if (entry.getValue() != null)
                                        values.put(entry.getKey(), entry.getValue());
                                return values;
                            }));
        }
        return result;
    }

    /**
     * Writes the same part set to several independent scopes in as few round trips as the store
     * supports. Externalized objects live in one scope each, so uploading the entries of one
     * commit (for example a model call's request/prepared and model/end payloads) would
     * otherwise cost one transaction per object.
     *
     * <p>The default keeps per-scope transactional semantics so existing stores stay correct;
     * stores sharing one connection should override it to commit all scopes at once. A null
     * part value deletes that part, matching {@link Transaction#put} implementations that
     * treat null as a deletion.
     */
    default void writeScopes(Map<String, Map<String, JsonNode>> writes) {
        if (writes == null || writes.isEmpty()) return;
        for (var scope : writes.entrySet()) {
            var parts = scope.getValue();
            if (parts == null || parts.isEmpty()) continue;
            transaction(
                    scope.getKey(),
                    tx -> {
                        for (var part : parts.entrySet()) tx.put(part.getKey(), part.getValue());
                        return null;
                    });
        }
    }

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
