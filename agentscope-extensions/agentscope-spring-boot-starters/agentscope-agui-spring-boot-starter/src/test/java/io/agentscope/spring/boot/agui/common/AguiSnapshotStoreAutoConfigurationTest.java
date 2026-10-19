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
package io.agentscope.spring.boot.agui.common;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.agentscope.core.agui.processor.AguiRequestProcessor;
import io.agentscope.core.agui.registry.AguiAgentRegistry;
import io.agentscope.core.agui.store.AguiSnapshotStore;
import io.agentscope.core.agui.store.InMemoryAguiSnapshotStore;
import io.agentscope.spring.boot.agui.mvc.AgentscopeAguiMvcAutoConfiguration;
import io.agentscope.spring.boot.agui.mvc.AguiMvcController;
import io.agentscope.spring.boot.agui.webflux.AgentscopeAguiWebFluxAutoConfiguration;
import io.agentscope.spring.boot.agui.webflux.AguiWebFluxHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for AG-UI snapshot store auto-configuration.
 */
@Tag("unit")
@DisplayName("AguiSnapshotStore Auto-configuration Unit Tests")
class AguiSnapshotStoreAutoConfigurationTest {

    private final WebApplicationContextRunner mvcContextRunner =
            new WebApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(AgentscopeAguiMvcAutoConfiguration.class))
                    .withUserConfiguration(RegistryConfig.class);

    private final ReactiveWebApplicationContextRunner webFluxContextRunner =
            new ReactiveWebApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(AgentscopeAguiWebFluxAutoConfiguration.class))
                    .withUserConfiguration(RegistryConfig.class);

    @Test
    @DisplayName("Should not create snapshot store beans by default for MVC")
    void testMvcNoSnapshotStoreByDefault() {
        mvcContextRunner.run(
                ctx -> {
                    ctx.getBean(AguiMvcController.class);
                    assertTrue(ctx.getBeansOfType(AguiSnapshotStore.class).isEmpty());
                    assertNoSnapshotCoordinator(mvcProcessor(ctx.getBean(AguiMvcController.class)));
                });
    }

    @Test
    @DisplayName("Should not create snapshot store beans by default for WebFlux")
    void testWebFluxNoSnapshotStoreByDefault() {
        webFluxContextRunner.run(
                ctx -> {
                    ctx.getBean(AguiWebFluxHandler.class);
                    assertTrue(ctx.getBeansOfType(AguiSnapshotStore.class).isEmpty());
                    assertNoSnapshotCoordinator(
                            webFluxProcessor(ctx.getBean(AguiWebFluxHandler.class)));
                });
    }

    @Test
    @DisplayName("Should wire in-memory snapshot store into the processor when enabled for MVC")
    void testMvcSnapshotStoreEnabled() {
        mvcContextRunner
                .withPropertyValues("agentscope.agui.snapshot-store-enabled=true")
                .run(
                        ctx -> {
                            AguiSnapshotStore store = ctx.getBean(AguiSnapshotStore.class);
                            assertTrue(store instanceof InMemoryAguiSnapshotStore);
                            AguiSnapshotStore wired =
                                    wiredStore(mvcProcessor(ctx.getBean(AguiMvcController.class)));
                            assertSame(store, wired);
                        });
    }

    @Test
    @DisplayName("Should wire in-memory snapshot store into the processor when enabled for WebFlux")
    void testWebFluxSnapshotStoreEnabled() {
        webFluxContextRunner
                .withPropertyValues("agentscope.agui.snapshot-store-enabled=true")
                .run(
                        ctx -> {
                            AguiSnapshotStore store = ctx.getBean(AguiSnapshotStore.class);
                            assertTrue(store instanceof InMemoryAguiSnapshotStore);
                            AguiSnapshotStore wired =
                                    wiredStore(
                                            webFluxProcessor(
                                                    ctx.getBean(AguiWebFluxHandler.class)));
                            assertSame(store, wired);
                        });
    }

    @Test
    @DisplayName("Should let a user-defined snapshot store bean take precedence for MVC")
    void testMvcUserSnapshotStoreTakesPrecedence() {
        mvcContextRunner
                .withPropertyValues("agentscope.agui.snapshot-store-enabled=true")
                .withUserConfiguration(SnapshotStoreConfig.class)
                .run(
                        ctx -> {
                            AguiSnapshotStore store = ctx.getBean(AguiSnapshotStore.class);
                            assertSame(SnapshotStoreConfig.STORE, store);
                            assertSame(
                                    SnapshotStoreConfig.STORE,
                                    wiredStore(mvcProcessor(ctx.getBean(AguiMvcController.class))));
                        });
    }

    @Test
    @DisplayName("Should let a user-defined snapshot store bean take precedence for WebFlux")
    void testWebFluxUserSnapshotStoreTakesPrecedence() {
        webFluxContextRunner
                .withPropertyValues("agentscope.agui.snapshot-store-enabled=true")
                .withUserConfiguration(SnapshotStoreConfig.class)
                .run(
                        ctx -> {
                            AguiSnapshotStore store = ctx.getBean(AguiSnapshotStore.class);
                            assertSame(SnapshotStoreConfig.STORE, store);
                            assertSame(
                                    SnapshotStoreConfig.STORE,
                                    wiredStore(
                                            webFluxProcessor(
                                                    ctx.getBean(AguiWebFluxHandler.class))));
                        });
    }

    @Test
    @DisplayName("Should keep snapshot store decoupled from serverSideMemory for MVC")
    void testMvcSnapshotStoreDecoupledFromServerSideMemory() {
        mvcContextRunner
                .withPropertyValues("agentscope.agui.server-side-memory=true")
                .run(
                        ctx -> {
                            ctx.getBean(AguiMvcController.class);
                            assertTrue(ctx.getBeansOfType(AguiSnapshotStore.class).isEmpty());
                        });
    }

    @Test
    @DisplayName("Should keep snapshot store decoupled from serverSideMemory for WebFlux")
    void testWebFluxSnapshotStoreDecoupledFromServerSideMemory() {
        webFluxContextRunner
                .withPropertyValues("agentscope.agui.server-side-memory=true")
                .run(
                        ctx -> {
                            ctx.getBean(AguiWebFluxHandler.class);
                            assertTrue(ctx.getBeansOfType(AguiSnapshotStore.class).isEmpty());
                        });
    }

    private static AguiRequestProcessor mvcProcessor(AguiMvcController controller) {
        return (AguiRequestProcessor) ReflectionTestUtils.getField(controller, "processor");
    }

    private static AguiRequestProcessor webFluxProcessor(AguiWebFluxHandler handler) {
        return (AguiRequestProcessor) ReflectionTestUtils.getField(handler, "processor");
    }

    private static void assertNoSnapshotCoordinator(AguiRequestProcessor processor) {
        assertTrue(ReflectionTestUtils.getField(processor, "snapshotCoordinator") == null);
    }

    private static AguiSnapshotStore wiredStore(AguiRequestProcessor processor) {
        Object coordinator = ReflectionTestUtils.getField(processor, "snapshotCoordinator");
        if (coordinator == null) {
            return null;
        }
        return (AguiSnapshotStore) ReflectionTestUtils.getField(coordinator, "store");
    }

    @Configuration
    static class RegistryConfig {

        @Bean
        public AguiAgentRegistry aguiAgentRegistry() {
            return new AguiAgentRegistry();
        }
    }

    @Configuration
    static class SnapshotStoreConfig {

        static final AguiSnapshotStore STORE = mock(AguiSnapshotStore.class);

        @Bean
        public AguiSnapshotStore aguiSnapshotStore() {
            return STORE;
        }
    }
}
