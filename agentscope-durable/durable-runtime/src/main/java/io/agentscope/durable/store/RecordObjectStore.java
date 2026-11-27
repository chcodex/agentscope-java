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
import io.agentscope.durable.DurableException;
import io.agentscope.durable.DurableJson;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/** Database fallback for small installations; production hosts may supply object storage. */
public final class RecordObjectStore implements ObjectStore {
    private final RecordStore records;

    public RecordObjectStore(RecordStore records) {
        this.records = Objects.requireNonNull(records);
    }

    public String id() {
        return "records";
    }

    private static final int CHUNK_BYTES = 1024 * 1024;

    /** Objects per batch; keeps one query well under the default 4 MiB packet limit. */
    private static final int OBJECT_BATCH = 50;

    /**
     * Upper bound of object bytes fetched in one batch. The chunk count of a batch is shared by
     * every key in it, so a page mixing one large object with many small ones would otherwise
     * probe every chunk slot for all of them.
     */
    private static final long OBJECT_BATCH_BYTES = 16L * 1024 * 1024;

    public Reference put(String scope, byte[] bytes) {
        var reference = ObjectStore.reference(id(), scope, bytes);
        records.transaction(
                "object:" + reference.key(),
                tx -> {
                    var existing = tx.get("");
                    if (existing != null) {
                        if (!reference.equals(DurableJson.convert(existing, Reference.class)))
                            throw new DurableException(
                                    DurableException.Code.CONFLICT, "Immutable object changed");
                        return null;
                    }
                    for (int offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
                        byte[] chunk =
                                Arrays.copyOfRange(
                                        bytes,
                                        offset,
                                        Math.min(bytes.length, offset + CHUNK_BYTES));
                        tx.put(
                                "chunk:" + offset / CHUNK_BYTES,
                                DurableJson.value(Base64.getEncoder().encodeToString(chunk)));
                    }
                    tx.put("", DurableJson.value(reference));
                    return null;
                });
        return reference;
    }

    public byte[] read(Reference reference) {
        checkStore(reference);
        return records.readTransaction(
                "object:" + reference.key(),
                tx -> {
                    var existing = tx.get("");
                    if (existing == null)
                        throw new DurableException(
                                DurableException.Code.NOT_FOUND, "Object not found");
                    return decode(reference, existing, offset -> tx.get("chunk:" + offset));
                });
    }

    /**
     * Batch form of {@link #read}: objects live in one scope each, so the plain path costs one
     * transaction per object. {@link RecordStore#readScopes} collapses a page into one query.
     */
    @Override
    public java.util.Map<Reference, byte[]> readAll(
            String scope, java.util.List<Reference> references) {
        if (references.isEmpty()) return java.util.Map.of();
        var result = new java.util.LinkedHashMap<Reference, byte[]>(references.size());
        int from = 0;
        while (from < references.size()) {
            int to = from;
            long bytes = 0;
            while (to < references.size()
                    && to - from < OBJECT_BATCH
                    && (to == from || bytes + references.get(to).size() <= OBJECT_BATCH_BYTES)) {
                bytes += references.get(to).size();
                to++;
            }
            var slice = references.subList(from, to);
            from = to;
            int maxChunks = 1;
            var keys = new java.util.ArrayList<String>(slice.size());
            for (var reference : slice) {
                checkStore(reference);
                if (!scope.equals(reference.scope()))
                    throw new DurableException(
                            DurableException.Code.INVALID, "Object belongs to another scope");
                keys.add("object:" + reference.key());
                maxChunks =
                        Math.max(
                                maxChunks,
                                (int) ((reference.size() + CHUNK_BYTES - 1) / CHUNK_BYTES));
            }
            var parts = new java.util.ArrayList<String>(maxChunks + 1);
            parts.add("");
            for (int i = 0; i < maxChunks; i++) parts.add("chunk:" + i);
            var loaded = records.readScopes(keys, parts);
            for (var reference : slice) {
                var values = loaded.get("object:" + reference.key());
                if (values == null || !values.containsKey(""))
                    throw new DurableException(DurableException.Code.NOT_FOUND, "Object not found");
                result.put(
                        reference,
                        decode(reference, values.get(""), offset -> values.get("chunk:" + offset)));
            }
        }
        return result;
    }

    private void checkStore(Reference reference) {
        if (!id().equals(reference.store()))
            throw new DurableException(DurableException.Code.INVALID, "Object store differs");
    }

    private byte[] decode(Reference reference, JsonNode header, ChunkReader chunks) {
        if (!reference.equals(DurableJson.convert(header, Reference.class)))
            throw new DurableException(DurableException.Code.INVALID, "Object header differs");
        var bytes = new ByteArrayOutputStream();
        for (long offset = 0; offset < reference.size(); offset += CHUNK_BYTES) {
            var chunk = chunks.get(offset / CHUNK_BYTES);
            if (chunk == null)
                throw new DurableException(DurableException.Code.INVALID, "Object chunk missing");
            byte[] decoded = Base64.getDecoder().decode(chunk.asText());
            if (decoded.length != Math.min(CHUNK_BYTES, reference.size() - offset))
                throw new DurableException(
                        DurableException.Code.INVALID, "Object chunk size differs");
            bytes.writeBytes(decoded);
        }
        return ObjectStore.verify(id(), reference, bytes.toByteArray());
    }

    @FunctionalInterface
    private interface ChunkReader {
        JsonNode get(long index);
    }
}
