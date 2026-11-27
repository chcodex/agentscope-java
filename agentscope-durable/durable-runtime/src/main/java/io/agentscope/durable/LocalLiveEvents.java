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
import io.agentscope.durable.DurableTypes.SessionKey;
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

        /** Session/Thread filter; null means all. */
        private final SessionKey session;

        private final String threadId;
        private final Set<String> seen = new LinkedHashSet<>();

        /** Live frames received while the replay window is being delivered. */
        private final ArrayDeque<LiveEvent> pending = new ArrayDeque<>();

        private boolean replaying;

        private Listener(Consumer<LiveEvent> callback) {
            this(callback, null, null);
        }

        private Listener(Consumer<LiveEvent> callback, SessionKey session, String threadId) {
            this.callback = callback;
            this.session = session;
            this.threadId = threadId;
        }

        private synchronized void deliver(LiveEvent event) {
            if (session != null
                    && (!session.equals(event.session())
                            || (threadId != null && !threadId.equals(event.threadId())))) return;
            if (replaying) {
                pending.addLast(event);
                return;
            }
            deliverNow(event);
        }

        private synchronized void beginReplay() {
            replaying = true;
        }

        private synchronized void endReplay() {
            replaying = false;
            while (!pending.isEmpty()) deliverNow(pending.removeFirst());
        }

        private void deliverNow(LiveEvent event) {
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

    /** Recent deltas of the current Step, keyed by Session and Thread. */
    private final Map<ThreadRef, Recent> recent = new LinkedHashMap<>();

    private final Object recentLock = new Object();

    private record ThreadRef(SessionKey session, String threadId) {}

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
    public AutoCloseable subscribe(
            SessionKey session, String threadId, Consumer<LiveEvent> callback) {
        Objects.requireNonNull(session, "session");
        if (threadId == null) {
            var listener = new Listener(Objects.requireNonNull(callback), session, null);
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }
        var listener = new Listener(Objects.requireNonNull(callback), session, threadId);
        // Mark the listener as replaying, snapshot and register under the lock, then deliver the
        // replay outside it. Live frames that publish during the replay are queued by the
        // listener and delivered after the snapshot, so a slow callback cannot stall other
        // publishers, subscribers or cleanup.
        listener.beginReplay();
        List<LiveEvent> replay;
        synchronized (recentLock) {
            var buffer = recent.get(new ThreadRef(session, threadId));
            replay = buffer == null ? List.of() : List.copyOf(buffer.events);
            listeners.add(listener);
        }
        try {
            for (var event : replay) {
                try {
                    listener.deliver(event);
                } catch (RuntimeException ignored) {
                    // Replay is best effort; the durable path is unaffected.
                }
            }
        } finally {
            try {
                listener.endReplay();
            } catch (RuntimeException failed) {
                // The caller's callback failed while the queued live frames were drained. Fail
                // the subscription instead of returning a handle for a listener that was removed.
                listeners.remove(listener);
                throw failed;
            }
        }
        return () -> listeners.remove(listener);
    }

    /**
     * Forget the replay window of a Thread whose Run has ended. The Run's committed events are the
     * source of truth; replaying its partial preview into the next Run produces orphaned starts.
     */
    @Override
    public void clear(SessionKey session, String threadId) {
        if (session == null || threadId == null) return;
        synchronized (recentLock) {
            recent.remove(new ThreadRef(session, threadId));
        }
    }

    private void remember(LiveEvent event) {
        try {
            synchronized (recentLock) {
                var buffer =
                        recent.computeIfAbsent(
                                new ThreadRef(event.session(), event.threadId()),
                                ignored -> new Recent());
                // Only a real Step transition replaces the preview window. Run-scoped live events
                // (for example x.result_metadata) carry a null stepId and must not wipe the
                // current Step's deltas.
                if (buffer.stepId != null
                        && event.stepId() != null
                        && !buffer.stepId.equals(event.stepId())) {
                    buffer.events.clear();
                    buffer.bytes = 0;
                }
                if (event.stepId() != null) buffer.stepId = event.stepId();
                int size = utf8Length(event);
                // A single oversized preview must not bypass the memory bound. Its committed
                // counterpart still reaches the client, so dropping the preview is safe.
                if (size > RECENT_BYTES) return;
                buffer.events.addLast(event);
                buffer.bytes += size;
                if (buffer.events.size() > RECENT_EVENTS || buffer.bytes > RECENT_BYTES) {
                    // Replaying the tail of a truncated preview would start mid-message (the
                    // message.started frame is gone). Drop the window instead: the committed
                    // message is the complete source and a resume starts a fresh preview.
                    buffer.events.clear();
                    buffer.bytes = 0;
                }
            }
        } catch (RuntimeException ignored) {
            // Replay is best effort; a malformed preview never affects the durable path.
        }
    }

    private static int utf8Length(LiveEvent event) {
        return event.data().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }
}
