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

/** Durable outbox cursor. A lost ACK re-delivers the same immutable event, never re-executes it. */
public final class JournalExporter {
    private final ThreadJournal log;
    private final JournalExportSink sink;

    public JournalExporter(ThreadJournal log, JournalExportSink sink) {
        this.log = log;
        this.sink = sink;
    }

    public synchronized void drain() {
        drain(sink.maxEventsPerDrain());
    }

    /**
     * Drains a bounded committed prefix, retaining the same durable acknowledgement rules.
     *
     * <p>The export watermark is advanced in batches instead of once per event: a cursor write is
     * a CAS round-trip on most backends, and a long journal would otherwise pay one write per
     * exported event. A lost ACK still re-delivers the whole unacknowledged tail, so sinks remain
     * idempotent by event id.
     */
    public synchronized long drain(long maxEvents) {
        if (maxEvents < 1) throw new IllegalArgumentException("Export limit must be positive");
        long cursor = log.exportCursor(sink.name());
        // The scan may start ahead of the stored cursor (see replayStart), but the acknowledgement
        // must CAS against the stored value: expecting the scan start would conflict forever on a
        // fresh journal whose cursor is still behind it.
        long from = Math.max(cursor, log.replayStart());
        long remaining = Math.max(0L, log.head().seq() - from);
        long through = from + Math.min(maxEvents, remaining);
        long last = cursor;
        int pending = 0;
        long count = 0;
        try {
            for (var event : log.scan(from, through)) {
                sink.accept(event);
                last = event.seq();
                count++;
                if (++pending >= 200) {
                    log.advanceExportCursor(sink.name(), cursor, last);
                    cursor = last;
                    pending = 0;
                }
            }
            if (pending > 0) log.advanceExportCursor(sink.name(), cursor, last);
            return count;
        } catch (RuntimeException error) {
            // A failed delivery must not acknowledge the failed event, but the prefix that was
            // already accepted is safe to record. Without this, a slow or failing sink would
            // replay the whole batch (up to 200 events) on every retry.
            if (last > cursor) {
                try {
                    log.advanceExportCursor(sink.name(), cursor, last);
                } catch (RuntimeException advance) {
                    error.addSuppressed(advance);
                }
            }
            throw error;
        }
    }
}
