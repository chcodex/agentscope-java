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

import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.util.JsonUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Latest-only presentation snapshot of one AG-UI thread.
 *
 * <p>AguiThreadSnapshot holds the AG-UI presentation state ({@code messages}, {@code state},
 * {@code activity}) that an AG-UI frontend needs to recover the conversation view after a refresh
 * or a reconnect. It is <b>not</b> execution state: AgentScope runtime state stays with the
 * framework, and open interrupts are tracked by the resume coordinator, not here.
 *
 * <p>Instances are deeply defensive copies: mutating the lists or maps passed to the factory after
 * the snapshot is built does not change the snapshot, and stored entries are not shared with other
 * snapshots.
 */
public record AguiThreadSnapshot(
        List<AguiMessage> messages,
        Map<String, Object> state,
        Map<String, ActivityEntry> activity) {

    /** Render activity stored for one message id. */
    public record ActivityEntry(String activityType, Map<String, Object> content) {
        public ActivityEntry {
            Objects.requireNonNull(activityType, "activityType cannot be null");
            content = content == null ? Map.of() : deepCopyMap(content);
        }
    }

    public AguiThreadSnapshot {
        messages = copyMessages(messages);
        state = state == null ? Map.of() : deepCopyMap(state);
        activity = copyActivity(activity);
    }

    /**
     * Creates a snapshot with explicit sections.
     *
     * @param messages conversation messages, may be null
     * @param state shared state map, may be null
     * @param activity render activities keyed by message id, may be null
     * @return a new immutable snapshot
     */
    public static AguiThreadSnapshot of(
            List<AguiMessage> messages,
            Map<String, Object> state,
            Map<String, ActivityEntry> activity) {
        return new AguiThreadSnapshot(messages, state, activity);
    }

    /**
     * Creates a snapshot with only messages.
     *
     * @param messages conversation messages
     * @return a new immutable snapshot
     */
    public static AguiThreadSnapshot ofMessages(List<AguiMessage> messages) {
        return new AguiThreadSnapshot(messages, null, null);
    }

    private static List<AguiMessage> copyMessages(List<AguiMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }
        List<AguiMessage> copy = new ArrayList<>(messages.size());
        for (AguiMessage message : messages) {
            if (message == null) {
                continue;
            }
            AguiMessage copied = deepCopy(message, AguiMessage.class);
            copy.add(copied != null ? copied : message);
        }
        return List.copyOf(copy);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopyMap(Map<String, Object> map) {
        try {
            String json = JsonUtils.getJsonCodec().toJson(map);
            Map<String, Object> copy = JsonUtils.getJsonCodec().fromJson(json, Map.class);
            if (copy != null) {
                return new LinkedHashMap<>(copy);
            }
        } catch (RuntimeException error) {
            // Fall through to the shallow copy.
        }
        return new LinkedHashMap<>(map);
    }

    private static Map<String, ActivityEntry> copyActivity(Map<String, ActivityEntry> activity) {
        if (activity == null || activity.isEmpty()) {
            return Map.of();
        }
        Map<String, ActivityEntry> copy = new LinkedHashMap<>();
        for (Map.Entry<String, ActivityEntry> entry : activity.entrySet()) {
            ActivityEntry value = entry.getValue();
            if (value == null) {
                continue;
            }
            copy.put(entry.getKey(), new ActivityEntry(value.activityType(), value.content()));
        }
        return Map.copyOf(copy);
    }

    @SuppressWarnings("unchecked")
    private static <T> T deepCopy(T value, Class<T> type) {
        try {
            String json = JsonUtils.getJsonCodec().toJson(value);
            return (T) JsonUtils.getJsonCodec().fromJson(json, type);
        } catch (RuntimeException error) {
            return value;
        }
    }
}
