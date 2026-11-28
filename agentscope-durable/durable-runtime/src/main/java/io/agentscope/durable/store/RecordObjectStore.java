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
                    for (var part : objectParts(reference, bytes).entrySet())
                        tx.put(part.getKey(), part.getValue());
                    return null;
                });
        return reference;
    }

    /**
     * Batch form of {@link #put}: one model call externalizes several payloads (for example its
     * request/prepared and model/end entries), and the plain path costs one transaction per
     * object. This collapses a batch into one {@link RecordStore#writeScopes} call, so a store
     * sharing one connection commits every object at once.
     *
     * <p>Semantics match {@link #put}: an existing object with a different header still fails
     * with {@code CONFLICT}, already stored objects are skipped without rewriting them, and
     * references keep the input order (duplicates included).
     *
     * <p>Single-writer only, like the rest of the journal: object keys carry the SHA-256 of
     * their bytes, so two batches racing on the same key necessarily carry identical bytes and
     * converge (last-writer-wins is a no-op). A different header under the same key would imply
     * store corruption or a hash collision, both outside the threat model, and still surfaces
     * as {@code CONFLICT} on the check-then-write path.
     */
    public java.util.List<Reference> putAll(String scope, java.util.List<byte[]> contents) {
        if (contents.isEmpty()) return java.util.List.of();
        if (contents.size() == 1) return java.util.List.of(put(scope, contents.get(0)));
        var references = new java.util.ArrayList<Reference>(contents.size());
        for (byte[] bytes : contents) references.add(ObjectStore.reference(id(), scope, bytes));
        var distinct = new java.util.LinkedHashMap<String, Reference>(references.size());
        var payloads = new java.util.LinkedHashMap<String, byte[]>(references.size());
        for (int i = 0; i < references.size(); i++)
            if (distinct.putIfAbsent(references.get(i).key(), references.get(i)) == null)
                payloads.put(references.get(i).key(), contents.get(i));
        var keys = new java.util.ArrayList<String>(distinct.size());
        for (var key : distinct.keySet()) keys.add("object:" + key);
        var stored = records.readScopes(keys, java.util.List.of(""));
        var writes = new java.util.LinkedHashMap<String, java.util.Map<String, JsonNode>>();
        for (var entry : distinct.entrySet()) {
            var reference = entry.getValue();
            var existing = stored.get("object:" + entry.getKey());
            var header = existing == null ? null : existing.get("");
            if (header != null) {
                if (!reference.equals(DurableJson.convert(header, Reference.class)))
                    throw new DurableException(
                            DurableException.Code.CONFLICT, "Immutable object changed");
                continue;
            }
            writes.put(
                    "object:" + entry.getKey(),
                    objectParts(reference, payloads.get(entry.getKey())));
        }
        if (!writes.isEmpty()) records.writeScopes(writes);
        return java.util.List.copyOf(references);
    }

    /**
     * Header plus chunk parts of one object. All parts of {@link #put} and {@link #putAll}
     * commit inside a single transaction per scope, so a large object never costs one
     * transaction per chunk.
     */
    private static java.util.LinkedHashMap<String, JsonNode> objectParts(
            Reference reference, byte[] bytes) {
        var parts = new java.util.LinkedHashMap<String, JsonNode>();
        for (int offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
            byte[] chunk =
                    Arrays.copyOfRange(bytes, offset, Math.min(bytes.length, offset + CHUNK_BYTES));
            parts.put(
                    "chunk:" + offset / CHUNK_BYTES,
                    DurableJson.value(Base64.getEncoder().encodeToString(chunk)));
        }
        parts.put("", DurableJson.value(reference));
        return parts;
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
