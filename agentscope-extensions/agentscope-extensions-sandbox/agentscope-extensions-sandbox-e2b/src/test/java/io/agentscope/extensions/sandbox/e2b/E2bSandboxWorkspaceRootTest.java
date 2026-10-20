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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import org.junit.jupiter.api.Test;

class E2bSandboxWorkspaceRootTest {

    @Test
    void createUsesWorkspaceSpecRootOverDefaultOptionRoot() {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot("/root/workspace");
        E2bSandboxClientOptions options = new E2bSandboxClientOptions();
        options.setApiKey("test-key");

        Sandbox sandbox = new E2bSandboxClient().create(spec, null, options);

        assertEquals("/root/workspace", ((E2bSandbox) sandbox).getWorkspaceRoot());
    }

    @Test
    void resumeUsesWorkspaceSpecRootForLegacyState() {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot("/root/workspace");
        E2bSandboxState state = new E2bSandboxState();
        state.setWorkspaceSpec(spec);
        state.setWorkspaceRoot("/home/user");

        Sandbox sandbox = new E2bSandboxClient().resume(state);

        assertEquals("/root/workspace", ((E2bSandbox) sandbox).getWorkspaceRoot());
        assertEquals("/root/workspace", state.getWorkspaceRoot());
    }

    @Test
    void filesystemSpecPropagatesWorkspaceSpecRoot() {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot("/root/workspace");
        E2bFilesystemSpec filesystemSpec = new E2bFilesystemSpec().workspaceSpec(spec);
        SandboxContext context = filesystemSpec.toSandboxContext();

        assertEquals("/root/workspace", context.getWorkspaceSpec().getRoot());

        Sandbox sandbox =
                new E2bSandboxClient()
                        .create(
                                context.getWorkspaceSpec(),
                                null,
                                (E2bSandboxClientOptions) context.getClientOptions());

        assertEquals("/root/workspace", ((E2bSandbox) sandbox).getWorkspaceRoot());
    }
}
