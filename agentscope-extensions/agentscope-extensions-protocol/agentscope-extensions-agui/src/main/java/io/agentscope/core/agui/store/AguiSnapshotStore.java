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

/**
 * Public SPI for storing AG-UI presentation snapshots keyed by AG-UI thread id.
 *
 * <p>This is a <b>State Store, not an Event Store</b>: implementations persist only the latest
 * {@link AguiThreadSnapshot} per thread (covering write), never an event sequence. The store feeds
 * hydrate flows, which replay the stored presentation state to a frontend as a read-only synthetic
 * AG-UI run.
 *
 * <p>Implementations must be thread-safe. The default in-memory implementation is bounded by an
 * access-order LRU. Snapshots are presentation-only and safe to lose: a durable implementation
 * (for restarts or multiple nodes) must apply the same covering-write contract.
 */
public interface AguiSnapshotStore {

    /**
     * Store the latest snapshot for a thread, replacing any previous snapshot.
     *
     * @param threadId the AG-UI thread id
     * @param snapshot the snapshot to store
     */
    void save(String threadId, AguiThreadSnapshot snapshot);

    /**
     * Get the latest snapshot for a thread.
     *
     * @param threadId the AG-UI thread id
     * @return the stored snapshot, or {@code null} when the thread has no snapshot
     */
    AguiThreadSnapshot get(String threadId);

    /**
     * Delete the snapshot of a thread.
     *
     * @param threadId the AG-UI thread id
     */
    void delete(String threadId);

    /** Delete all stored snapshots. */
    void clear();
}
