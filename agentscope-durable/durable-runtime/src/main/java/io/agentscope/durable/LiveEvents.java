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
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Best-effort fan-out for live presentation events. It is deliberately separate from
 * {@link SessionBackend}: publishing never creates durable history, cursors or projections.
 */
public interface LiveEvents {
    /** No live delivery. Durable execution and committed event history remain unaffected. */
    static LiveEvents none() {
        return new LiveEvents() {
            @Override
            public void publish(LiveEvent event) {}

            @Override
            public AutoCloseable subscribe(Consumer<LiveEvent> listener) {
                return () -> {};
            }
        };
    }

    /** Thread-safe process-local fan-out suitable for an integrated Brain/API deployment. */
    static LiveEvents local() {
        return new LocalLiveEvents();
    }

    void publish(LiveEvent event);

    AutoCloseable subscribe(Consumer<LiveEvent> listener);

    /**
     * Drops the replay window of one Thread once its Run is over.
     *
     * <p>A parked or finished Run must not replay its previews to the next subscriber: the
     * resumed Run starts fresh, and the interrupted Run's output is already covered by its
     * committed events. Leaving the buffer in place makes a resume connection receive orphaned
     * {@code TEXT_MESSAGE_START}/{@code TOOL_CALL_START} frames whose matching end frames were
     * committed (and therefore skipped by the new cursor).
     *
     * <p>The Session is part of the identity: Thread ids are only unique inside one Session.
     */
    default void clear(SessionKey session, String threadId) {}

    /**
     * Subscribes for one Thread and first replays the recent live events still held in memory, so
     * a reconnect can resume the current partial message without committing every delta. The
     * replay is best effort and process-local, exactly like {@link #publish}.
     *
     * <p>Only events of the given Session and Thread are delivered; implementations that keep no
     * per-Thread window are filtered here.
     */
    default AutoCloseable subscribe(
            SessionKey session, String threadId, Consumer<LiveEvent> listener) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(listener, "listener");
        return subscribe(
                event -> {
                    if (session.equals(event.session())
                            && (threadId == null || threadId.equals(event.threadId())))
                        listener.accept(event);
                });
    }
}
