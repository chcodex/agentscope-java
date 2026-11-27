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
package io.agentscope.core.message;

/**
 * Single source of truth for the effective state of a tool result.
 *
 * <p>{@link ToolResultBlock} defaults an unset state to {@link ToolResultState#RUNNING}, so a tool
 * that finished successfully without calling {@code withState} looks unfinished to any reader that
 * maps the raw state. Execution and event layer must agree on one interpretation; deriving it here
 * keeps them consistent.
 */
public final class ToolResultStates {

    private ToolResultStates() {}

    /**
     * Effective state of a finished tool result.
     *
     * <p>A suspended block stays {@link ToolResultState#SUSPENDED}. An explicitly marked state is
     * kept as-is. Otherwise the default {@link ToolResultState#RUNNING} is read as "not declared":
     * output starting with {@code [ERROR]} means {@link ToolResultState#ERROR}, anything else is
     * {@link ToolResultState#SUCCESS}.
     */
    public static ToolResultState resolve(ToolResultBlock result) {
        if (result.isSuspended()) {
            return ToolResultState.SUSPENDED;
        }
        if (result.getState() != null && result.getState() != ToolResultState.RUNNING) {
            return result.getState();
        }
        if (result.getOutput() != null
                && result.getOutput().stream()
                        .anyMatch(
                                block ->
                                        block instanceof TextBlock text
                                                && text.getText() != null
                                                && text.getText().startsWith("[ERROR]"))) {
            return ToolResultState.ERROR;
        }
        return ToolResultState.SUCCESS;
    }
}
