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
package io.agentscope.extensions.sandbox.e2b;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.agentscope.harness.agent.sandbox.SandboxException;
import java.io.Serializable;
import java.util.Objects;

/**
 * A pre-existing E2B volume attached at sandbox creation time.
 *
 * <p>{@code name} is the existing volume name and {@code path} is the absolute in-sandbox mount
 * path. The mounts are sent as {@code volumeMounts: [{name, path}]} on {@code POST /sandboxes} and
 * re-attached automatically whenever the sandbox is recreated.
 */
public final class E2bVolumeMount implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String path;

    /**
     * Creates a volume mount.
     *
     * @param name existing volume name (blank rejected)
     * @param path absolute in-sandbox mount path (blank or relative rejected)
     */
    @JsonCreator
    public E2bVolumeMount(@JsonProperty("name") String name, @JsonProperty("path") String path) {
        if (name == null || name.isBlank()) {
            throw new SandboxException.SandboxConfigurationException(
                    "E2B volume mount name is required");
        }
        if (path == null || path.isBlank()) {
            throw new SandboxException.SandboxConfigurationException(
                    "E2B volume mount path is required");
        }
        if (!path.startsWith("/")) {
            throw new SandboxException.SandboxConfigurationException(
                    "E2B volume mount path must be absolute: " + path);
        }
        this.name = name;
        this.path = path;
    }

    /**
     * Returns the existing volume name.
     *
     * @return volume name (never blank)
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the absolute in-sandbox mount path.
     *
     * @return mount path (never blank, always absolute)
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns whether a mount covers the workspace root, i.e. the workspace lives on the volume.
     *
     * @param workspaceRoot workspace root path (null/blank never covered)
     * @param mountPath volume mount path (null/blank never covers)
     * @return true when {@code mountPath} equals {@code workspaceRoot} or is one of its ancestors
     */
    public static boolean coversWorkspaceRoot(String workspaceRoot, String mountPath) {
        if (workspaceRoot == null
                || workspaceRoot.isBlank()
                || mountPath == null
                || mountPath.isBlank()) {
            return false;
        }
        String root = stripTrailingSlash(workspaceRoot.replace('\\', '/'));
        String mount = stripTrailingSlash(mountPath.replace('\\', '/'));
        return mount.equals(root) || root.startsWith(mount + "/");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof E2bVolumeMount other)) {
            return false;
        }
        return name.equals(other.name) && path.equals(other.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path);
    }

    @Override
    public String toString() {
        return "E2bVolumeMount{name='" + name + "', path='" + path + "'}";
    }

    private static String stripTrailingSlash(String s) {
        if (s.length() > 1 && s.endsWith("/")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }
}
