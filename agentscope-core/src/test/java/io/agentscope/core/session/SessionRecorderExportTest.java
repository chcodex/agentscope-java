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
package io.agentscope.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SessionRecorderExportTest {

    private static final long WAIT_SECONDS = 5;
    // The heartbeat retries exports every second, so a shorter window proves the drain loop.
    private static final long DRAIN_LOOP_WAIT_MILLIS = 800;

    @Test
    void exportRequestedWhileDrainingIsExportedBeforeClose() throws Exception {
        var sink = new BlockingSink();
        var recorder = recorder(sink);
        try {
            recorder.recordNow("presentation/hint", Map.of("value", 1));
            assertTrue(
                    sink.firstAcceptStarted.await(WAIT_SECONDS, TimeUnit.SECONDS),
                    "the first drain should reach the sink");
            // The drain is blocked in the sink, so this request races the in-flight export.
            recorder.recordNow("presentation/hint", Map.of("value", 2));
            sink.releaseFirstAccept.countDown();
            assertTrue(
                    sink.secondAccepted.await(DRAIN_LOOP_WAIT_MILLIS, TimeUnit.MILLISECONDS),
                    "a drain request must be exported before close and the heartbeat retry");
            assertEquals(List.of(1L, 2L), sink.accepted);
        } finally {
            sink.releaseFirstAccept.countDown();
            recorder.close();
        }
        assertNull(sink.failure.get());
    }

    @Test
    void closeExportsRequestMadeWhileDraining() throws Exception {
        var sink = new BlockingSink();
        var recorder = recorder(sink);
        try {
            recorder.recordNow("presentation/hint", Map.of("value", 1));
            assertTrue(
                    sink.firstAcceptStarted.await(WAIT_SECONDS, TimeUnit.SECONDS),
                    "the first drain should reach the sink");
            recorder.recordNow("presentation/hint", Map.of("value", 2));
            recorder.close();
        } finally {
            sink.releaseFirstAccept.countDown();
        }
        assertTrue(
                sink.secondAccepted.await(WAIT_SECONDS, TimeUnit.SECONDS),
                "close must export a request made while a drain was running");
        assertEquals(List.of(1L, 2L), sink.accepted);
        assertNull(sink.failure.get());
    }

    private static SessionRecorder recorder(BlockingSink sink) {
        var context = RuntimeContext.builder().userId("u").sessionId("s").build();
        var log = new InMemorySessionLogStore().open(new SessionKey("u", "agent", "s"), context);
        return new SessionRecorder(log, "turn", "run", sink);
    }

    /** Blocks the first accept so a second export request races the in-flight drain. */
    private static final class BlockingSink implements SessionExportSink {
        private final List<Long> accepted = new CopyOnWriteArrayList<>();
        private final CountDownLatch firstAcceptStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstAccept = new CountDownLatch(1);
        private final CountDownLatch secondAccepted = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public String name() {
            return "test-sink";
        }

        @Override
        public void accept(SessionEvent event) {
            accepted.add(event.seq());
            if (accepted.size() == 1) {
                firstAcceptStarted.countDown();
                try {
                    if (!releaseFirstAccept.await(WAIT_SECONDS, TimeUnit.SECONDS))
                        failure.compareAndSet(
                                null, new AssertionError("first accept was not released"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure.compareAndSet(null, e);
                }
            }
            if (accepted.size() == 2) secondAccepted.countDown();
        }
    }
}
