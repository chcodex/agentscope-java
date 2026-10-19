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

import io.agentscope.core.agui.event.AguiEvent.JsonPatchOperation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal RFC 6902 JSON Patch applier for AG-UI {@code STATE_DELTA} / {@code ACTIVITY_DELTA}
 * materialization.
 *
 * <p>Supports the operation subset the AG-UI protocol uses for snapshot deltas: {@code add},
 * {@code remove}, and {@code replace} over nested objects and arrays. Operations whose pointer does
 * not resolve are skipped (fail-safe) so a single malformed delta cannot corrupt materialization.
 */
final class AguiJsonPatch {

    private static final String ARRAY_END = "-";

    private AguiJsonPatch() {}

    /**
     * Applies patch operations to a deep copy of the target map.
     *
     * @param target baseline state map, may be null
     * @param operations patch operations, may be null
     * @return a new writable map with the operations applied
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> apply(
            Map<String, Object> target, List<JsonPatchOperation> operations) {
        Map<String, Object> root = copy(target == null ? Map.of() : target);
        if (operations == null || operations.isEmpty()) {
            return root;
        }
        for (JsonPatchOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            List<String> tokens = contextTokens(operation.path());
            if (tokens == null) {
                continue;
            }
            String op = operation.op();
            if ("add".equals(op)) {
                applyAdd(reference(root, tokens, true), operation.value());
            } else if ("replace".equals(op)) {
                applyReplace(reference(root, tokens, false), operation.value());
            } else if ("remove".equals(op)) {
                applyRemove(reference(root, tokens, false));
            }
        }
        return root;
    }

    private static void applyAdd(Reference target, Object value) {
        if (target == null) {
            return;
        }
        Object container = target.container();
        if (container instanceof Map<?, ?>) {
            ((Map<String, Object>) container).put(target.token(), value);
        } else if (container instanceof List<?> list && target.index() != null) {
            List<Object> mutable = (List<Object>) list;
            if (target.index() >= mutable.size()) {
                mutable.add(value);
            } else {
                mutable.add(target.index(), value);
            }
        }
    }

    private static void applyReplace(Reference target, Object value) {
        if (target == null) {
            return;
        }
        Object container = target.container();
        if (container instanceof Map<?, ?>) {
            ((Map<String, Object>) container).put(target.token(), value);
        } else if (container instanceof List<?> list
                && target.index() != null
                && target.index() < list.size()) {
            ((List<Object>) list).set(target.index(), value);
        }
    }

    private static void applyRemove(Reference target) {
        if (target == null) {
            return;
        }
        Object container = target.container();
        if (container instanceof Map<?, ?>) {
            ((Map<String, Object>) container).remove(target.token());
        } else if (container instanceof List<?> list
                && target.index() != null
                && target.index() < list.size()) {
            ((List<Object>) list).remove(target.index().intValue());
        }
    }

    /**
     * Reference to the container node addressed by a pointer, with the final token as map key or
     * list index. {@code -} ("array end") only resolves for add operations.
     */
    private record Reference(Object container, String token, Integer index) {}

    /** Unescaped pointer tokens, or null when the pointer is unresolvable. */
    private static List<String> contextTokens(String pointer) {
        if (pointer == null || pointer.isEmpty() || pointer.charAt(0) != '/') {
            return null;
        }
        String[] parts = pointer.substring(1).split("/", -1);
        List<String> tokens = new ArrayList<>(parts.length);
        for (String part : parts) {
            tokens.add(part.replace("~1", "/").replace("~0", "~"));
        }
        return tokens;
    }

    private static Reference reference(
            Map<String, Object> root, List<String> tokens, boolean allowArrayEnd) {
        Object node = root;
        for (int i = 0; i < tokens.size() - 1; i++) {
            node = child(node, tokens.get(i));
            if (node == null) {
                return null;
            }
        }
        String token = tokens.get(tokens.size() - 1);
        return new Reference(node, token, listIndex(token, allowArrayEnd));
    }

    @SuppressWarnings("unchecked")
    private static Object child(Object node, String token) {
        if (node instanceof Map<?, ?>) {
            return ((Map<String, Object>) node).get(token);
        }
        if (node instanceof List<?> list) {
            Integer index = listIndex(token, false);
            return index != null && index < list.size() ? list.get(index) : null;
        }
        return null;
    }

    private static Integer listIndex(String token, boolean allowArrayEnd) {
        if (ARRAY_END.equals(token)) {
            return allowArrayEnd ? Integer.MAX_VALUE : null;
        }
        if (token.isEmpty() || !Character.isDigit(token.charAt(0))) {
            return null;
        }
        try {
            int index = Integer.parseInt(token);
            return index >= 0 ? index : null;
        } catch (NumberFormatException error) {
            return null;
        }
    }

    /** Deep copies JSON-like structures (maps and lists); scalar leaves are shared. */
    @SuppressWarnings("unchecked")
    private static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), deepCopy(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(deepCopy(item));
            }
            return copy;
        }
        return value;
    }

    private static Map<String, Object> copy(Map<String, Object> target) {
        return (Map<String, Object>) deepCopy(target);
    }
}
