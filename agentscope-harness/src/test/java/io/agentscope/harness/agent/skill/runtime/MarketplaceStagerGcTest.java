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
package io.agentscope.harness.agent.skill.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.skill.AgentSkill;
import io.agentscope.core.skill.repository.AgentSkillRepository;
import io.agentscope.core.skill.repository.AgentSkillRepositoryInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Focused tests for {@link MarketplaceStager#stage(List, Map)} orphan GC behaviour.
 *
 * <p>Staged layout is {@code .skills-cache/&lt;scope&gt;/&lt;source-ns&gt;/&lt;skill&gt;} and GC
 * only reclaims entries untouched for a full grace window, so deletion assertions backdate the
 * orphan's mtime instead of expecting immediate removal.
 */
class MarketplaceStagerGcTest {

    @TempDir Path tempWorkspace;

    private static final String SKILL_MD = "---\nname: test\n---";

    private static Path stagedFile(Path workspace, String ns, String skill, String file) {
        return workspace
                .resolve(MarketplaceStager.CACHE_DIR)
                .resolve(MarketplaceStager.SHARED_SCOPE)
                .resolve(ns)
                .resolve(skill)
                .resolve(file);
    }

    private static Path skillDir(Path workspace, String ns, String skill) {
        return stagedFile(workspace, ns, skill, "x").getParent();
    }

    // ==================== Flat namespace ====================

    @Test
    @DisplayName("Flat namespace: skill survives staging")
    void flatNamespace_skillSurvives() {
        AgentSkill skill =
                new AgentSkill(
                        "my-skill",
                        "desc",
                        "c",
                        Map.of("f.txt", "hello", "SKILL.md", SKILL_MD),
                        "src");
        StubRepo repo = new StubRepo(List.of(skill), "src");
        MarketplaceStager stager = new MarketplaceStager(tempWorkspace);

        stager.stage(List.of(new MarketplaceStager.RepoBound(skill, repo)), Map.of(repo, "src"));

        assertTrue(Files.exists(stagedFile(tempWorkspace, "src", "my-skill", "f.txt")));
    }

    @Test
    @DisplayName("Flat namespace: stale orphan skill is deleted on re-stage")
    void flatNamespace_orphanDeleted() throws IOException {
        AgentSkill skillA =
                new AgentSkill(
                        "skill-a", "desc", "c", Map.of("a.txt", "a", "SKILL.md", SKILL_MD), "src");
        StubRepo repo = new StubRepo(List.of(skillA), "src");
        MarketplaceStager stager = new MarketplaceStager(tempWorkspace, Duration.ofHours(6));

        // First stage: materialise skill-a
        stager.stage(List.of(new MarketplaceStager.RepoBound(skillA, repo)), Map.of(repo, "src"));

        Path stagedA = stagedFile(tempWorkspace, "src", "skill-a", "a.txt");
        assertTrue(Files.exists(stagedA), "skill-a should be staged initially");

        // Backdate so the orphan is past the grace window
        Files.setLastModifiedTime(
                skillDir(tempWorkspace, "src", "skill-a"),
                FileTime.from(Instant.now().minus(Duration.ofDays(7))));

        // Second stage: repo no longer publishes skill-a → orphan GC deletes it
        stager.stage(List.of(), Map.of(repo, "src"));

        assertFalse(Files.exists(stagedA), "orphan skill-a should be deleted by GC");
        assertTrue(
                Files.notExists(
                        tempWorkspace
                                .resolve(MarketplaceStager.CACHE_DIR)
                                .resolve(MarketplaceStager.SHARED_SCOPE)
                                .resolve("src")),
                "empty namespace dir should also be cleaned up");
    }

    @Test
    @DisplayName("Flat namespace: fresh orphan survives within the grace window")
    void flatNamespace_freshOrphanSurvives() {
        AgentSkill skillA =
                new AgentSkill(
                        "skill-a", "desc", "c", Map.of("a.txt", "a", "SKILL.md", SKILL_MD), "src");
        AgentSkill skillB =
                new AgentSkill(
                        "skill-b", "desc", "c", Map.of("b.txt", "b", "SKILL.md", SKILL_MD), "src");
        StubRepo repo = new StubRepo(List.of(skillA, skillB), "src");
        MarketplaceStager stager = new MarketplaceStager(tempWorkspace);

        stager.stage(
                List.of(
                        new MarketplaceStager.RepoBound(skillA, repo),
                        new MarketplaceStager.RepoBound(skillB, repo)),
                Map.of(repo, "src"));

        Path stagedA = stagedFile(tempWorkspace, "src", "skill-a", "a.txt");
        Path stagedB = stagedFile(tempWorkspace, "src", "skill-b", "b.txt");
        assertTrue(Files.exists(stagedA));
        assertTrue(Files.exists(stagedB));

        // Re-stage with only skill-a: skill-b just staged, still fresh → kept
        stager.stage(List.of(new MarketplaceStager.RepoBound(skillA, repo)), Map.of(repo, "src"));

        assertTrue(Files.exists(stagedA), "retained skill-a should survive");
        assertTrue(Files.exists(stagedB), "fresh orphan skill-b should survive the grace window");
    }

    // ==================== Mixed namespaces ====================

    @Test
    @DisplayName("Mixed flat namespaces: all retained skills survive")
    void mixedNamespaces_allRetainedSurvive() {
        AgentSkill flat =
                new AgentSkill(
                        "flat", "desc", "c", Map.of("f.txt", "f", "SKILL.md", SKILL_MD), "src");
        AgentSkill other =
                new AgentSkill(
                        "other", "desc", "c", Map.of("d.txt", "d", "SKILL.md", SKILL_MD), "market");
        StubRepo flatRepo = new StubRepo(List.of(flat), "src");
        StubRepo otherRepo = new StubRepo(List.of(other), "market");
        MarketplaceStager stager = new MarketplaceStager(tempWorkspace);

        stager.stage(
                List.of(
                        new MarketplaceStager.RepoBound(flat, flatRepo),
                        new MarketplaceStager.RepoBound(other, otherRepo)),
                Map.of(flatRepo, "src", otherRepo, "market"));

        assertTrue(Files.exists(stagedFile(tempWorkspace, "src", "flat", "f.txt")));
        assertTrue(Files.exists(stagedFile(tempWorkspace, "market", "other", "d.txt")));
    }

    // ==================== Legacy directory without SKILL.md ====================

    @Test
    @DisplayName("Legacy dir without SKILL.md: freshly created dir survives the grace window")
    void legacyDirWithoutSkillMd_notRecognizedAsSkill() throws IOException {
        AgentSkill skillA =
                new AgentSkill(
                        "skill-a", "desc", "c", Map.of("a.txt", "a", "SKILL.md", SKILL_MD), "src");
        StubRepo repo = new StubRepo(List.of(skillA), "src");
        MarketplaceStager stager = new MarketplaceStager(tempWorkspace);

        stager.stage(List.of(new MarketplaceStager.RepoBound(skillA, repo)), Map.of(repo, "src"));

        Path stagedA = stagedFile(tempWorkspace, "src", "skill-a", "a.txt");
        assertTrue(Files.exists(stagedA));

        // Manually create a legacy directory with no SKILL.md
        Path legacyDir =
                tempWorkspace
                        .resolve(MarketplaceStager.CACHE_DIR)
                        .resolve(MarketplaceStager.SHARED_SCOPE)
                        .resolve("src")
                        .resolve("legacy-skill");
        Files.createDirectories(legacyDir.resolve("residual.txt"));

        // Re-stage with only skill-a → GC runs, legacy dir is fresh so the grace
        // window keeps it
        stager.stage(List.of(new MarketplaceStager.RepoBound(skillA, repo)), Map.of(repo, "src"));

        assertTrue(Files.exists(stagedA), "skill-a should survive");
        assertTrue(
                Files.exists(legacyDir.resolve("residual.txt")),
                "fresh legacy dir should survive the grace window");
    }

    /** Minimal repository stub for testing. */
    private static final class StubRepo implements AgentSkillRepository {

        private final List<AgentSkill> skills;
        private final String source;

        StubRepo(List<AgentSkill> skills, String source) {
            this.skills = skills;
            this.source = source;
        }

        @Override
        public AgentSkill getSkill(String name) {
            return skills.stream().filter(s -> s.getName().equals(name)).findFirst().orElse(null);
        }

        @Override
        public List<String> getAllSkillNames() {
            return skills.stream().map(AgentSkill::getName).toList();
        }

        @Override
        public List<AgentSkill> getAllSkills() {
            return skills;
        }

        @Override
        public boolean save(List<AgentSkill> skills, boolean force) {
            return false;
        }

        @Override
        public boolean delete(String skillName) {
            return false;
        }

        @Override
        public boolean skillExists(String skillName) {
            return skills.stream().anyMatch(s -> s.getName().equals(skillName));
        }

        @Override
        public AgentSkillRepositoryInfo getRepositoryInfo() {
            return new AgentSkillRepositoryInfo(source, "", false);
        }

        @Override
        public String getSource() {
            return source;
        }

        @Override
        public void setWriteable(boolean writeable) {}

        @Override
        public boolean isWriteable() {
            return false;
        }
    }
}
