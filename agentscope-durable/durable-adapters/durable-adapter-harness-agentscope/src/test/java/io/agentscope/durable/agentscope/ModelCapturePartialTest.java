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
package io.agentscope.durable.agentscope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ModelCapturePartialTest {
    private static ModelCapture capture() {
        return new ModelCapture(
                "fixture",
                "REASONING",
                List.of(),
                List.of(),
                GenerateOptions.builder().build(),
                "hash");
    }

    private static ChatResponse chunk(String id, String text) {
        return ChatResponse.builder()
                .id(id)
                .content(List.of(TextBlock.builder().text(text).build()))
                .build();
    }

    @Test
    void cancelledCallKeepsAssembledPartialInsteadOfChunks() {
        var capture = capture();
        capture.nextAttempt();
        capture.accept(chunk("m-1", "hello"));
        capture.accept(chunk("m-1", " world"));

        @SuppressWarnings("unchecked")
        var outcome = (Map<String, Object>) capture.freeze("cancelled");
        assertEquals(true, outcome.get("interrupted"));
        @SuppressWarnings("unchecked")
        var partial = (Map<String, Object>) outcome.get("partial");
        assertEquals("hello world", partial.get("text"));
        assertEquals(List.of(), outcome.get("responses"));
    }

    @Test
    void completedCallKeepsFullResponseWithoutPartial() {
        var capture = capture();
        capture.nextAttempt();
        capture.accept(chunk("m-1", "done"));

        @SuppressWarnings("unchecked")
        var outcome = (Map<String, Object>) capture.freeze("completed");
        assertFalse(outcome.containsKey("partial"));
        assertFalse(outcome.containsKey("interrupted"));
        assertEquals(1, ((List<?>) outcome.get("responses")).size());
    }

    @Test
    void cancelledCallWithoutContentCarriesNoPartial() {
        var capture = capture();
        capture.nextAttempt();

        @SuppressWarnings("unchecked")
        var outcome = (Map<String, Object>) capture.freeze("cancelled");
        assertFalse(outcome.containsKey("partial"));
        assertTrue(outcome.containsKey("responses"));
    }
}
