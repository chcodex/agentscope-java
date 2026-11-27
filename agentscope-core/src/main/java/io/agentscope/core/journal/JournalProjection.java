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

import io.agentscope.core.agent.AgentExecutionPosition;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultMessage;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.core.util.MessageUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Read-only recovery view. Inspection never repairs records or invokes models/tools. */
public record JournalProjection(
        long asOfSeq,
        String stateJson,
        Set<String> uncertainToolCalls,
        Set<String> activeRuns,
        String positionTurnId,
        String positionRunId,
        AgentExecutionPosition executionPosition) {
    public static JournalProjection read(ThreadJournal log) {
        return read(log, event -> {});
    }

    /** Builds one projection while allowing startup readers to inspect that same metadata prefix. */
    public static JournalProjection read(ThreadJournal log, Consumer<JournalEvent> observer) {
        return read(log, null, observer);
    }

    /**
     * Reads a recovery view. The current seed does not contain all facts needed to reconstruct
     * tool outcomes and run history, so it is not used as a replay boundary. The observer sees
     * the complete committed prefix, just as in {@link #read(ThreadJournal, Consumer)}.
     */
    public static JournalProjection read(
            ThreadJournal log, ThreadJournal.ProjectionSeed seed, Consumer<JournalEvent> observer) {
        long cursor = 0;
        String state = null;
        JournalEvent checkpoint = null;
        String positionTurnId = null;
        String positionRunId = null;
        var uncertain = new LinkedHashSet<String>();
        var runs = new LinkedHashSet<String>();
        var results = new LinkedHashMap<String, ToolResultBlock>();
        var verifications = new ArrayList<io.agentscope.core.state.TaskVerification>();
        var extensions = new ArrayList<JournalEvent>();
        long upper = log.head().seq();
        // Checkpoints can repeat an earlier state and do not fold all recovery facts into it.
        for (var event : log.scanMetadata(cursor, upper)) {
            JournalEventTypes.validate(event);
            observer.accept(event);
            var data = event.data();
            switch (event.type()) {
                case "state/checkpoint" -> {
                    checkpoint = event;
                    // A periodic checkpoint carries no run identity; keep the last known one.
                    if (event.turnId() != null) positionTurnId = event.turnId();
                    if (event.executionRunId() != null) positionRunId = event.executionRunId();
                    verifications.clear();
                    extensions.clear();
                }
                case "state/initialized", "migration/baseline", "state/restored" -> {
                    checkpoint = event;
                    // A new baseline starts a new execution identity; inheriting the previous
                    // turn/run would attach unrelated calls to the old execution position.
                    positionTurnId = event.turnId();
                    positionRunId = event.executionRunId();
                    verifications.clear();
                    extensions.clear();
                }
                case "verification/result" -> {
                    if (data.get("verification") != null)
                        verifications.add(
                                JsonUtils.getJsonCodec()
                                        .convertValue(
                                                data.get("verification"),
                                                io.agentscope.core.state.TaskVerification.class));
                }
                case "tool/dispatch" -> uncertain.add((String) data.get("toolCallId"));
                case "action/end" -> {
                    if (data.get("result") != null) {
                        String id = (String) data.get("toolCallId");
                        var result =
                                JsonUtils.getJsonCodec()
                                        .convertValue(data.get("result"), ToolResultBlock.class);
                        if (result.hasUnknownOutcome()
                                || uncertain.contains(id)
                                        && result.getState() == ToolResultState.INTERRUPTED) {
                            uncertain.add(id);
                            results.remove(id);
                        } else {
                            uncertain.remove(id);
                            results.put(id, result);
                        }
                    }
                }
                case "recovery/applied" -> {
                    Object interrupted = data.get("interruptedRuns");
                    if (interrupted instanceof List<?> ids) ids.forEach(runs::remove);
                    Object resolved = data.get("resolvedToolCallIds");
                    if (resolved instanceof List<?> ids) ids.forEach(uncertain::remove);
                }
                case "run/start" -> runs.add(event.executionRunId());
                case "run/end" -> runs.remove(event.executionRunId());
                default -> {
                    if (!JournalEventTypes.BUILTIN.contains(event.type())) extensions.add(event);
                }
            }
            cursor = event.seq();
        }
        AgentExecutionPosition position = null;
        if (checkpoint != null) {
            var data = log.materialize(checkpoint).data();
            state = (String) data.get("stateJson");
            if (data.get("executionPosition") != null)
                position =
                        JsonUtils.getJsonCodec()
                                .convertValue(
                                        data.get("executionPosition"),
                                        AgentExecutionPosition.class);
        }
        if (state != null
                && (!results.isEmpty() || !verifications.isEmpty() || !extensions.isEmpty())) {
            AgentState restored = AgentState.fromJsonString(state);
            var existing = new HashSet<String>();
            for (var message : restored.getContext())
                for (var result : message.getContentBlocks(ToolResultBlock.class))
                    existing.add(result.getId());
            var pending = MessageUtils.pendingToolUseIds(restored.getContext());
            for (var entry : results.entrySet())
                if (pending.contains(entry.getKey())
                        && !existing.contains(entry.getKey())
                        && !entry.getValue().isSuspended())
                    restored.contextMutable()
                            .add(
                                    ToolResultMessage.builder()
                                            .id("recovered_" + entry.getKey())
                                            .result(entry.getValue())
                                            .build());
            for (var report : verifications)
                restored.getTasksContext()
                        .recordVerification(
                                report, restored.getTasksContext().snapshot().getRevision());
            for (var extension : extensions)
                JournalEventCodecRegistry.defaultRegistry()
                        .applyExtension(restored, log.materialize(extension));
            state = restored.toJson();
        }
        return new JournalProjection(
                cursor,
                state,
                Set.copyOf(uncertain),
                Set.copyOf(runs),
                positionTurnId,
                positionRunId,
                position);
    }

    public AgentState restore() {
        return stateJson == null ? null : AgentState.fromJsonString(stateJson);
    }
}
