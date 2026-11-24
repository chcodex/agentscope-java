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
package io.agentscope.core.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class JournalExporterBatchTest {

    private static final class MemoryStorage implements AtomicJournalStorage {
        private final Map<String, Value> values = new ConcurrentHashMap<>();
        int exportWrites;

        public Value read(String path) {
            return values.get(path);
        }

        public synchronized boolean compareAndSet(String path, long version, byte[] bytes) {
            var old = values.get(path);
            if ((old == null ? 0 : old.version()) != version) return false;
            if (path.contains("/exports/")) exportWrites++;
            values.put(path, new Value(version + 1, bytes));
            return true;
        }
    }

    @Test
    void advancesTheExportCursorEveryTwoHundredEventsAndReplaysOnlyTheTailAfterACrash() {
        var storage = new MemoryStorage();
        var log = new AtomicThreadJournal(storage, "batch");
        var writer = log.acquire("w", Duration.ofMinutes(2));
        long expected = 0;
        for (int i = 1; i <= 450; i++) {
            var event =
                    new JournalEvent(
                            JournalEvent.SCHEMA_VERSION,
                            "e" + i,
                            i,
                            1,
                            "run/start",
                            "r",
                            "t",
                            true,
                            "{}");
            log.commit(writer, "b" + i, expected, List.of(event));
            expected = i;
        }
        log.release(writer);

        var accepted = new ArrayList<Long>();
        var crash = new boolean[] {true};
        JournalExportSink sink =
                new JournalExportSink() {
                    public String name() {
                        return "batch-sink";
                    }

                    public void accept(JournalEvent event) {
                        if (crash[0] && event.seq() == 250) {
                            crash[0] = false;
                            throw new IllegalStateException("simulated sink crash");
                        }
                        accepted.add(event.seq());
                    }
                };
        var exporter = new JournalExporter(log, sink);

        assertThrows(IllegalStateException.class, exporter::drain);
        // The accepted prefix is acknowledged even when a later event fails; the failed event
        // itself is not acknowledged and is re-delivered on the next drain.
        assertEquals(249, log.exportCursor("batch-sink"));
        assertEquals(2, storage.exportWrites);

        exporter.drain();
        assertEquals(450, log.exportCursor("batch-sink"));
        assertEquals(4, storage.exportWrites);

        var distinct = new TreeSet<>(accepted);
        assertEquals(450, distinct.size());
        assertEquals(1, distinct.first());
        assertEquals(450, distinct.last());
    }
}
