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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SessionRecorderExportTest {

    private static final long WAIT_SECONDS = 5;

    @Test
    void exportRequestedWhileDrainingIsNotDropped() throws Exception {
        var context = RuntimeContext.builder().userId("u").sessionId("s").build();
        var log = new InMemorySessionLogStore().open(new SessionKey("u", "agent", "s"), context);
        var firstAcceptStarted = new CountDownLatch(1);
        var releaseFirstAccept = new CountDownLatch(1);
        var secondAccepted = new CountDownLatch(1);
        var accepted = new CopyOnWriteArrayList<Long>();
        SessionExportSink sink =
                new SessionExportSink() {
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
                                releaseFirstAccept.await(WAIT_SECONDS, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        if (accepted.size() == 2) secondAccepted.countDown();
                    }
                };
        var recorder = new SessionRecorder(log, "turn", "run", sink);
        try {
            recorder.recordNow("presentation/hint", Map.of("value", 1));
            assertTrue(
                    firstAcceptStarted.await(WAIT_SECONDS, TimeUnit.SECONDS),
                    "the first drain should reach the sink");
            // The drain is blocked in the sink, so this request races the in-flight export.
            recorder.recordNow("presentation/hint", Map.of("value", 2));
            recorder.close();
        } finally {
            releaseFirstAccept.countDown();
        }

        assertTrue(
                secondAccepted.await(WAIT_SECONDS, TimeUnit.SECONDS),
                "an export requested during a drain must not be dropped");
        assertEquals(List.of(1L, 2L), accepted);
    }
}
