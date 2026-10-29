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

import io.agentscope.core.agui.adapter.AguiAdapterConfig;
import io.agentscope.core.agui.event.AguiFramedEvent;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.session.SessionLog;
import reactor.core.publisher.Flux;

/**
 * Replays a {@link SessionLog} as AG-UI events, for callers that resolve the log themselves.
 *
 * <p>This is the entry point for applications whose AG-UI agent registry does not expose a
 * {@code HarnessAgent}, for example when a placeholder agent delegates execution to custom code.
 * It performs a pure replay after the caller-supplied cursor: it never starts or resumes an Agent
 * run and never builds a message snapshot. The application must supply the conversation history
 * up to the same cursor.
 *
 * @see AguiRequestProcessor#attach(io.agentscope.core.agui.runtime.AguiRuntimeContextRequest)
 */
public final class AguiSessionLogReplay {

    private AguiSessionLogReplay() {}

    /**
     * Replays committed session events after a cursor.
     *
     * @param input the AG-UI input; only {@code threadId}, {@code runId} and {@code tools} are used
     * @param log the session log to observe
     * @param after the last sequence already represented by the application history
     * @param executionRunId the session log execution run id, or {@code null} to follow any active
     *     run
     * @param config the adapter config controlling replay detail
     * @return framed AG-UI replay events, ending when the run finishes
     */
    public static Flux<AguiFramedEvent> replay(
            RunAgentInput input,
            SessionLog log,
            long after,
            String executionRunId,
            AguiAdapterConfig config) {
        return AguiSessionLogAttach.attach(input, log, after, executionRunId, config);
    }
}
