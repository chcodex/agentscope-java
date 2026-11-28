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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.agentscope.durable.DurableTypes.LiveEvent;
import io.agentscope.durable.DurableTypes.SessionKey;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-process best-effort fan-out. A slow listener must hand work off immediately.
 *
 * <p>Besides fan-out this keeps a small replay window per Thread so a reconnect within the same
 * process can resume the current Step. Deltas of one block are merged into a single accumulated
 * entry: the window holds the text once instead of the whole sequence, so a reconnect receives the
 * current text in one frame and then continues with the live deltas. The committed
 * {@code message.completed} remains the only source of truth and replaces the preview.
 *
 * <p>Accounting uses character counts rather than serializing payloads, so recording an event
 * costs one map lookup and one append on the streaming thread.
 */
final class LocalLiveEvents implements LiveEvents {
    /**
     * Maximum accumulated characters per Thread. Exceeding it drops the offending block instead of
     * truncating: a preview that starts mid-message would be rendered as if it were the whole text.
     */
    private static final int RECENT_CHARS = 256 * 1024;

    /**
     * Bounded ring of tool output previews; older chunks are dropped and text is unaffected.
     */
    private static final int TOOL_OUTPUT_EVENTS = 128;

    /**
     * Maximum Base64 payload kept per tool output preview. Tool outputs are opaque chunks, so
     * unlike text blocks they cannot be accumulated; an oversized single frame is dropped instead
     * of retained, keeping the per-thread replay window bounded while live fan-out is unaffected.
     */
    private static final int TOOL_OUTPUT_MAX_CHARS = 64 * 1024;

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

    private final Map<ThreadRef, Recent> recent = new ConcurrentHashMap<>();

    private record ThreadRef(SessionKey session, String threadId) {}

    /**
     * One accumulated block, materialized as a single delta when a subscriber reconnects.
     */
    private static final class Accumulated {
        private final LiveEvent template;
        private final StringBuilder text = new StringBuilder();
        private int chars;

        private Accumulated(LiveEvent template) {
            this.template = template;
        }

        private void append(String value) {
            text.append(value);
            chars += value.length();
        }
    }

    private static final class Recent {
        private String stepId;
        private final Map<String, Accumulated> blocks = new LinkedHashMap<>();
        private final ArrayDeque<LiveEvent> toolOutput = new ArrayDeque<>();
        private final Set<String> dropped = new LinkedHashSet<>();
        private int chars;
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
        Objects.requireNonNull(callback, "callback");
        if (threadId == null) {
            var listener = new Listener(callback, session, null);
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }
        var listener = new Listener(callback, session, threadId);
        // Snapshot and register under the same monitor that guards the window, then deliver the
        // replay outside it. Frames published during the replay are queued by the listener, so a
        // slow callback cannot stall other publishers and no delta is lost or repeated.
        listener.beginReplay();
        List<LiveEvent> replay;
        // Snapshot and register atomically: remember() takes the same monitor when it creates
        // and fills a window, so a frame published between the lookup and the registration is
        // either in the replay or queued by the already-replaying listener — never lost.
        synchronized (recent) {
            var buffer = recent.get(new ThreadRef(session, threadId));
            if (buffer == null) {
                replay = List.of();
                listeners.add(listener);
            } else {
                synchronized (buffer) {
                    replay = snapshot(buffer);
                    listeners.add(listener);
                }
            }
        }
        for (var event : replay) {
            try {
                listener.deliver(event);
            } catch (RuntimeException ignored) {
                // Replay is best effort; the durable path is unaffected.
            }
        }
        try {
            listener.endReplay();
        } catch (RuntimeException failed) {
            // The caller's callback failed while the queued live frames were drained. Fail the
            // subscription instead of returning a handle for a listener that was removed.
            listeners.remove(listener);
            throw failed;
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
        recent.remove(new ThreadRef(session, threadId));
    }

    private void remember(LiveEvent event) {
        if (event.threadId() == null) return;
        try {
            // Same monitor as subscribe(): creation and fill are atomic against snapshot and
            // registration, so a concurrent subscriber can neither miss this frame nor see a
            // half-filled window.
            synchronized (recent) {
                var buffer =
                        recent.computeIfAbsent(
                                new ThreadRef(event.session(), event.threadId()),
                                ignored -> new Recent());
                synchronized (buffer) {
                    // Only a real Step transition replaces the window. Run-scoped live events carry
                    // a
                    // null stepId and must not wipe the current Step's preview.
                    if (buffer.stepId != null
                            && event.stepId() != null
                            && !buffer.stepId.equals(event.stepId())) {
                        buffer.blocks.clear();
                        buffer.toolOutput.clear();
                        buffer.dropped.clear();
                        buffer.chars = 0;
                    }
                    if (event.stepId() != null) buffer.stepId = event.stepId();
                    switch (event.type()) {
                        case "message.delta", "tool.arguments.delta" -> accumulate(buffer, event);
                        case "tool.output.delta" -> {
                            if (event.data().path("data").asText("").length()
                                    > TOOL_OUTPUT_MAX_CHARS) break;
                            buffer.toolOutput.addLast(event);
                            if (buffer.toolOutput.size() > TOOL_OUTPUT_EVENTS)
                                buffer.toolOutput.removeFirst();
                        }
                        default -> {}
                    }
                }
            }
        } catch (RuntimeException ignored) {
            // Replay is best effort; a malformed preview never affects the durable path.
        }
    }

    private static void accumulate(Recent buffer, LiveEvent event) {
        String key = blockKey(event);
        String text = event.data().path("text").asText("");
        if (text.isEmpty()) return;
        var block = buffer.blocks.get(key);
        if (block == null) {
            if (buffer.dropped.contains(key)) return;
            block = new Accumulated(event);
            buffer.blocks.put(key, block);
        }
        if (buffer.chars + text.length() > RECENT_CHARS) {
            // Starting a preview mid-message would show a fragment as if it were the whole text.
            buffer.blocks.remove(key);
            buffer.dropped.add(key);
            buffer.chars -= block.chars;
            return;
        }
        block.append(text);
        buffer.chars += text.length();
    }

    private static String blockKey(LiveEvent event) {
        var data = event.data();
        if ("tool.arguments.delta".equals(event.type()))
            return "arguments:" + data.path("toolCallId").asText("");
        return event.type()
                + ":"
                + data.path("messageId").asText("")
                + ":"
                + data.path("blockId").asText("");
    }

    /**
     * One accumulated delta per block followed by the retained tool output previews.
     */
    private static List<LiveEvent> snapshot(Recent buffer) {
        var replay = new ArrayList<LiveEvent>(buffer.blocks.size() + buffer.toolOutput.size());
        for (var entry : buffer.blocks.entrySet())
            replay.add(materialize(entry.getKey(), entry.getValue()));
        replay.addAll(buffer.toolOutput);
        return replay;
    }

    private static LiveEvent materialize(String key, Accumulated block) {
        var data = (ObjectNode) block.template.data().deepCopy();
        data.put("text", block.text.toString());
        return new LiveEvent(
                block.template.schemaVersion(),
                // A distinct id: the live stream never emitted this frame, so per-listener
                // deduplication must not discard the replay.
                "replay:" + key + ":" + block.chars,
                block.template.session(),
                block.template.threadId(),
                block.template.turnId(),
                block.template.runId(),
                block.template.stepId(),
                block.template.occurredAt(),
                block.template.type(),
                data);
    }
}
