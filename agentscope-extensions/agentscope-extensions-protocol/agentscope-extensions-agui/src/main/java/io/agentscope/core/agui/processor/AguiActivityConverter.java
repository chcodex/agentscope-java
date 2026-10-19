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
package io.agentscope.core.agui.processor;

import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.store.AguiThreadSnapshot.ActivityEntry;
import java.util.Map;
import java.util.Objects;

/**
 * Materializes AG-UI activity events into the {@code activity} section of a snapshot.
 *
 * <p>Mirrors {@code AguiStateConverter} for {@code ACTIVITY_SNAPSHOT} / {@code ACTIVITY_DELTA}
 * events. Activities are keyed by the AG-UI message id the activity belongs to.
 */
final class AguiActivityConverter {

    private AguiActivityConverter() {}

    /**
     * Replaces the stored activity of one message with the snapshot content.
     *
     * @param activity activity entries keyed by message id
     * @param event the incoming {@code ACTIVITY_SNAPSHOT} event
     */
    static void applySnapshot(
            Map<String, ActivityEntry> activity, AguiEvent.ActivitySnapshot event) {
        Objects.requireNonNull(activity, "activity cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        activity.put(event.messageId(), new ActivityEntry(event.activityType(), event.content()));
    }

    /**
     * Applies an {@code ACTIVITY_DELTA} patch to the stored activity of one message.
     *
     * <p>A delta for an unknown message id materializes an empty baseline first, so the patch can
     * still be captured instead of being dropped.
     *
     * @param activity activity entries keyed by message id
     * @param event the incoming {@code ACTIVITY_DELTA} event
     */
    static void applyDelta(Map<String, ActivityEntry> activity, AguiEvent.ActivityDelta event) {
        Objects.requireNonNull(activity, "activity cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        ActivityEntry baseline =
                activity.computeIfAbsent(
                        event.messageId(), id -> new ActivityEntry(event.activityType(), Map.of()));
        Map<String, Object> content = AguiJsonPatch.apply(baseline.content(), event.patch());
        activity.put(event.messageId(), new ActivityEntry(event.activityType(), content));
    }
}
