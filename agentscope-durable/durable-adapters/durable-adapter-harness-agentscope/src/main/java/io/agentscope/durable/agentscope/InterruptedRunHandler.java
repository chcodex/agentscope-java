/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.agentscope.durable.agentscope;

import io.agentscope.durable.DurableTypes.ThreadKey;
import io.agentscope.durable.DurableTypes.ThreadSpec;
import io.agentscope.durable.DurableTypes.TurnStatus;
import io.agentscope.durable.DurableTypes.TurnView;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Decides whether a native invocation left unfinished by a previous host is resumed.
 *
 * <p>The adapter reports the fact (a {@code run/start} without its {@code run/end}) and the host
 * decides what to do with it. Returning {@link Optional#empty()} keeps the automatic resume that
 * Durable has always performed; returning a {@link Settlement} ends the Turn instead and leaves
 * the reclaimed context in the journal for a later Turn.
 *
 * <p>{@link #RESUME} is the default and preserves the previous behavior. Nothing here decides
 * policy on behalf of the application: it only exposes the decision point.
 */
@FunctionalInterface
public interface InterruptedRunHandler {

    /** Keeps the framework's automatic resume. */
    InterruptedRunHandler RESUME = run -> Optional.empty();

    /**
     * @param run facts about the interrupted native execution
     * @return an empty value to resume, or the terminal outcome that settles the Turn
     */
    Optional<Settlement> decide(InterruptedRun run);

    /** A native run recorded by the previous host that never reached {@code run/end}. */
    record InterruptedRun(
            ThreadKey thread, ThreadSpec definition, TurnView turn, Set<String> activeRunIds) {
        public InterruptedRun {
            Objects.requireNonNull(thread, "thread");
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(turn, "turn");
            activeRunIds = Set.copyOf(Objects.requireNonNull(activeRunIds, "activeRunIds"));
        }
    }

    /** Terminal status used when the host declines to resume. */
    record Settlement(TurnStatus status, String reason) {
        public Settlement {
            if (status != TurnStatus.CANCELLED && status != TurnStatus.FAILED) {
                throw new IllegalArgumentException(
                        "Settlement requires TurnStatus.CANCELLED or TurnStatus.FAILED");
            }
            Objects.requireNonNull(reason, "reason");
        }
    }
}
