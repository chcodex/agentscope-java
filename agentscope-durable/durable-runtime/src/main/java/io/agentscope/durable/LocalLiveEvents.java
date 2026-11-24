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
package io.agentscope.durable;

import io.agentscope.durable.DurableTypes.LiveEvent;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-process best-effort fan-out. A slow listener must hand work off immediately.
 *
 * <p>Besides fan-out this keeps a small replay window per Thread: the most recent streaming
 * deltas of the current Step. A reconnect within the same process can therefore resume the
 * partial message without every delta being committed; the committed {@code message.completed}
 * remains the only source of truth and replaces the preview.
 */
final class LocalLiveEvents implements LiveEvents {
    /** Upper bound of replayed deltas per Thread; protects memory for long Steps. */
    private static final int RECENT_EVENTS = 2000;

    /** Upper bound of replayed payload size per Thread. */
    private static final int RECENT_BYTES = 1 << 20;

    private static final int SEEN_EVENTS = 4096;

    /**
     * A listener deduplicates by native event id: publish remembers before fan-out and subscribe
     * replays before registering, so a concurrent publish can otherwise reach the new listener
     * twice (once from the replay window, once from the fan-out).
     */
    private static final class Listener {
        private final Consumer<LiveEvent> callback;
        private final Set<String> seen = new LinkedHashSet<>();

        private Listener(Consumer<LiveEvent> callback) {
            this.callback = callback;
        }

        private synchronized void deliver(LiveEvent event) {
            if (!seen.add(event.eventId())) return;
            if (seen.size() > SEEN_EVENTS) {
                var oldest = seen.iterator();
                oldest.next();
                oldest.remove();
            }
            callback.accept(event);
        }
    }

    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    /** Recent deltas of the current Step, keyed by Thread. Guarded by {@link #recentLock}. */
    private final Map<String, Recent> recent = new LinkedHashMap<>();

    private final Object recentLock = new Object();

    private static final class Recent {
        private String stepId;
        private final ArrayDeque<LiveEvent> events = new ArrayDeque<>();
        private int bytes;
    }

    @Override
    public void publish(LiveEvent event) {
        Objects.requireNonNull(event);
        remember(event);
        RuntimeException failure = null;
        for (var listener : listeners) {
            try {
                listener.deliver(event);
            } catch (RuntimeException error) {
                if (failure == null) failure = error;
                else if (failure != error) failure.addSuppressed(error);
            }
        }
        if (failure != null) throw failure;
    }

    @Override
    public AutoCloseable subscribe(Consumer<LiveEvent> callback) {
        var listener = new Listener(Objects.requireNonNull(callback));
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    @Override
    public AutoCloseable subscribe(String threadId, Consumer<LiveEvent> callback) {
        if (threadId == null) return subscribe(callback);
        var listener = new Listener(Objects.requireNonNull(callback));
        synchronized (recentLock) {
            // Replay before registering so a concurrent publish cannot slip between the two and
            // be delivered twice or not at all.
            var buffer = recent.get(threadId);
            if (buffer != null)
                for (var event : List.copyOf(buffer.events)) {
                    try {
                        listener.deliver(event);
                    } catch (RuntimeException ignored) {
                        // Replay is best effort; the durable path is unaffected.
                    }
                }
            listeners.add(listener);
        }
        return () -> listeners.remove(listener);
    }

    private void remember(LiveEvent event) {
        try {
            synchronized (recentLock) {
                var buffer = recent.computeIfAbsent(event.threadId(), ignored -> new Recent());
                if (buffer.stepId != null && !buffer.stepId.equals(event.stepId())) {
                    buffer.events.clear();
                    buffer.bytes = 0;
                }
                buffer.stepId = event.stepId();
                int size = event.data().toString().length();
                buffer.events.addLast(event);
                buffer.bytes += size;
                while (buffer.events.size() > RECENT_EVENTS
                        || (buffer.bytes > RECENT_BYTES && buffer.events.size() > 1)) {
                    var dropped = buffer.events.removeFirst();
                    buffer.bytes -= dropped.data().toString().length();
                }
            }
        } catch (RuntimeException ignored) {
            // Replay is best effort; a malformed preview never affects the durable path.
        }
    }
}
