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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agui.model.AguiMessage;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link InMemoryAguiSnapshotStore}. */
@Tag("unit")
@DisplayName("InMemoryAguiSnapshotStore Unit Tests")
class InMemoryAguiSnapshotStoreTest {

    private static AguiThreadSnapshot snapshot(String marker) {
        return AguiThreadSnapshot.ofMessages(List.of(AguiMessage.userMessage("msg-1", marker)));
    }

    @Test
    @DisplayName("Covering write replaces the previous snapshot")
    void saveOverwritesPreviousSnapshot() {
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();

        store.save("thread-1", snapshot("first"));
        assertEquals("first", store.get("thread-1").messages().get(0).getTextContent());

        store.save("thread-1", snapshot("second"));
        assertEquals("second", store.get("thread-1").messages().get(0).getTextContent());
        assertEquals(1, store.size());
    }

    @Test
    @DisplayName("delete and clear remove stored snapshots")
    void deleteAndClear() {
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore();
        store.save("thread-1", snapshot("one"));
        store.save("thread-2", snapshot("two"));

        store.delete("thread-1");
        assertNull(store.get("thread-1"));
        assertNotNull(store.get("thread-2"));

        store.clear();
        assertNull(store.get("thread-2"));
        assertEquals(0, store.size());
    }

    @Test
    @DisplayName("LRU eviction removes the least recently accessed thread, no resize scan")
    void lruEvictionAccessOrder() {
        // Capacity 2 with access-order eviction: a read of thread-1 must refresh its recency so
        // a later save evicts thread-2 instead of thread-1.
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore(2);
        store.save("thread-1", snapshot("one"));
        store.save("thread-2", snapshot("two"));
        assertNotNull(store.get("thread-1")); // refresh recency of thread-1

        store.save("thread-3", snapshot("three"));

        assertNotNull(store.get("thread-1"));
        assertNull(store.get("thread-2"));
        assertNotNull(store.get("thread-3"));
        assertEquals(2, store.size());
    }

    @Test
    @DisplayName("Concurrent saves and reads stay consistent")
    void concurrentAccess() throws Exception {
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore(64);
        ConcurrentLinkedQueue<String> markers = new ConcurrentLinkedQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        int writers = 8;
        int writesPerWriter = 64;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        for (int w = 0; w < writers; w++) {
            executor.execute(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < writesPerWriter; i++) {
                                String marker = "write-" + Thread.currentThread().getId() + "-" + i;
                                store.save("shared", snapshot(marker));
                                AguiThreadSnapshot read = store.get("shared");
                                if (read != null) {
                                    markers.add(read.messages().get(0).getTextContent());
                                }
                            }
                        } catch (InterruptedException interrupt) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        executor.shutdownNow();

        // Every read must observe a full marker string, never a torn value.
        for (String marker : markers) {
            assertTrue(marker.startsWith("write-"));
        }
        AguiThreadSnapshot last = store.get("shared");
        assertNotNull(last);
        assertEquals(
                last.messages().get(0).getTextContent(),
                markers.stream().reduce((a, b) -> b).orElse("missing"));
    }

    @Test
    @DisplayName("Capacity is clamped to at least one thread")
    void capacityClamp() {
        InMemoryAguiSnapshotStore store = new InMemoryAguiSnapshotStore(0);
        store.save("thread-1", snapshot("one"));
        assertNotNull(store.get("thread-1"));
        assertEquals(1, store.size());
    }
}
