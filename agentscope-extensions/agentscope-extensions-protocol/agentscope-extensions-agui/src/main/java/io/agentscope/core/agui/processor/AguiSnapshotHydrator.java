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
import io.agentscope.core.agui.store.AguiThreadSnapshot;
import io.agentscope.core.agui.store.AguiThreadSnapshot.ActivityEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Replays a stored presentation snapshot as a read-only synthetic AG-UI run.
 *
 * <p>Hydration never invokes an Agent or an LLM. The frame sequence is:
 *
 * <pre>
 * RUN_STARTED → [STATE_SNAPSHOT] → [MESSAGES_SNAPSHOT] → ACTIVITY_SNAPSHOT* → RUN_FINISHED(interrupts?)
 * </pre>
 *
 * <p>Interrupt metadata is presentation-level only: {@code RUN_FINISHED} carries the still
 * unresolved interrupt outcome so the frontend can re-render the approval UI, while the
 * authoritative HITL state stays with the resume coordinator and the Agent runtime.
 */
final class AguiSnapshotHydrator {

    private AguiSnapshotHydrator() {}

    /**
     * Builds the hydrate event lifecycle for one thread.
     *
     * @param threadId the AG-UI thread id
     * @param runId the synthetic run id provided by the client
     * @param snapshot the stored presentation snapshot
     * @param unresolvedInterrupts interrupts still open on the thread, in stable order
     * @return the hydrate event lifecycle, never null and never empty
     */
    static List<AguiEvent> hydrateEvents(
            String threadId,
            String runId,
            AguiThreadSnapshot snapshot,
            List<AguiEvent.Interrupt> unresolvedInterrupts) {
        List<AguiEvent> events = new ArrayList<>();
        events.add(new AguiEvent.RunStarted(threadId, runId, null, null));

        Map<String, Object> state = snapshot == null ? null : snapshot.state();
        if (state != null && !state.isEmpty()) {
            events.add(new AguiEvent.StateSnapshot(threadId, runId, state));
        }

        List<io.agentscope.core.agui.model.AguiMessage> messages =
                snapshot == null ? null : snapshot.messages();
        if (messages != null && !messages.isEmpty()) {
            events.add(new AguiEvent.MessagesSnapshot(threadId, runId, messages));
        }

        for (Map.Entry<String, ActivityEntry> entry : activityEntries(snapshot).entrySet()) {
            ActivityEntry value = entry.getValue();
            events.add(
                    new AguiEvent.ActivitySnapshot(
                            threadId,
                            runId,
                            entry.getKey(),
                            value.activityType(),
                            value.content()));
        }

        List<AguiEvent.Interrupt> interrupts =
                unresolvedInterrupts == null ? List.of() : List.copyOf(unresolvedInterrupts);
        AguiEvent.RunFinishedOutcome outcome =
                interrupts.isEmpty() ? null : new AguiEvent.RunFinishedInterruptOutcome(interrupts);
        events.add(new AguiEvent.RunFinished(threadId, runId, null, outcome));
        return List.copyOf(events);
    }

    private static Map<String, ActivityEntry> activityEntries(AguiThreadSnapshot snapshot) {
        Map<String, ActivityEntry> activity = snapshot == null ? null : snapshot.activity();
        if (activity == null || activity.isEmpty()) {
            return Map.of();
        }
        return new TreeMap<>(activity);
    }
}
