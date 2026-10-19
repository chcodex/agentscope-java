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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory, bounded, latest-only {@link AguiSnapshotStore}.
 *
 * <p>Entries are stored per thread with a covering write. When the capacity is reached, the least
 * recently accessed thread is evicted (access-order LRU), so reads don't elongate the map and
 * repeated saves never trigger a full map scan.
 */
public class InMemoryAguiSnapshotStore implements AguiSnapshotStore {

    /** Default maximum number of stored threads. */
    public static final int DEFAULT_CAPACITY = 1000;

    private final int capacity;
    private final Map<String, AguiThreadSnapshot> snapshots;
    private final Object lock = new Object();

    /** Creates a store with the default capacity. */
    public InMemoryAguiSnapshotStore() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a store with a bounded capacity.
     *
     * @param capacity maximum number of threads kept in memory; smaller values are raised to 1
     */
    public InMemoryAguiSnapshotStore(int capacity) {
        this.capacity = Math.min(Integer.MAX_VALUE - 1, Math.max(1, capacity));
        this.snapshots = newAccessOrderMap();
    }

    private LinkedHashMap<String, AguiThreadSnapshot> newAccessOrderMap() {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, AguiThreadSnapshot> eldest) {
                return size() > capacity;
            }
        };
    }

    @Override
    public void save(String threadId, AguiThreadSnapshot snapshot) {
        Objects.requireNonNull(threadId, "threadId cannot be null");
        Objects.requireNonNull(snapshot, "snapshot cannot be null");
        synchronized (lock) {
            snapshots.put(threadId, snapshot);
        }
    }

    @Override
    public AguiThreadSnapshot get(String threadId) {
        Objects.requireNonNull(threadId, "threadId cannot be null");
        synchronized (lock) {
            return snapshots.get(threadId);
        }
    }

    @Override
    public void delete(String threadId) {
        Objects.requireNonNull(threadId, "threadId cannot be null");
        synchronized (lock) {
            snapshots.remove(threadId);
        }
    }

    @Override
    public void clear() {
        synchronized (lock) {
            snapshots.clear();
        }
    }

    /** Number of threads currently stored. */
    public int size() {
        synchronized (lock) {
            return snapshots.size();
        }
    }
}
