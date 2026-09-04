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
package io.agentscope.harness.agent.tool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.tool.AgentTool;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.harness.agent.filesystem.AbstractFilesystem;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.model.EditResult;
import io.agentscope.harness.agent.filesystem.model.FileData;
import io.agentscope.harness.agent.filesystem.model.FileInfo;
import io.agentscope.harness.agent.filesystem.model.GlobResult;
import io.agentscope.harness.agent.filesystem.model.GrepMatch;
import io.agentscope.harness.agent.filesystem.model.GrepResult;
import io.agentscope.harness.agent.filesystem.model.LsResult;
import io.agentscope.harness.agent.filesystem.model.ReadResult;
import io.agentscope.harness.agent.workspace.WorkspacePathNormalizer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link FilesystemTool}. */
class FilesystemToolTest {

    private static final RuntimeContext RT = RuntimeContext.empty();

    private AbstractFilesystem filesystem;
    private FilesystemTool tool;

    @BeforeEach
    void setUp() {
        filesystem = mock(AbstractFilesystem.class);
        tool = new FilesystemTool(filesystem);
    }

    // ---------------------------------------------------------------
    // listFiles — 底层 ls() 已修复: 区分"不存在"/"不是目录"/"空目录"
    // ---------------------------------------------------------------

    @Test
    void listFiles_pathDoesNotExist_returnsErrorMessage() {
        when(filesystem.ls(RT, "/nonexistent"))
                .thenReturn(LsResult.fail("Path does not exist: /nonexistent"));

        String result = tool.listFiles(RT, "/nonexistent");

        assertEquals("Error: Path does not exist: /nonexistent", result);
    }

    @Test
    void listFiles_pathIsFileNotDirectory_returnsErrorMessage() {
        when(filesystem.ls(RT, "/some-file.txt"))
                .thenReturn(LsResult.fail("Not a directory: /some-file.txt"));

        String result = tool.listFiles(RT, "/some-file.txt");

        assertEquals("Error: Not a directory: /some-file.txt", result);
    }

    @Test
    void listFiles_emptyDirectory_returnsEmptyDirectoryMessage() {
        when(filesystem.ls(RT, "/empty-dir")).thenReturn(LsResult.success(List.of()));

        String result = tool.listFiles(RT, "/empty-dir");

        assertEquals("Empty directory: /empty-dir", result);
    }

    @Test
    void listFiles_nonEmptyDirectory_returnsFormattedEntries() {
        when(filesystem.ls(RT, "/dir"))
                .thenReturn(
                        LsResult.success(
                                List.of(
                                        FileInfo.ofDir("subdir", "2025-01-01"),
                                        FileInfo.ofFile("readme.md", 123L, "2025-01-01"),
                                        FileInfo.ofFile("data.csv", 456L, "2025-01-01"))));

        String result = tool.listFiles(RT, "/dir");

        String[] lines = result.split("\n");
        assertEquals(3, lines.length);
        assertTrue(lines[0].startsWith("[DIR]  subdir"), "目录项应有 [DIR] 前缀");
        assertTrue(lines[1].startsWith("[FILE] readme.md (123 bytes)"));
        assertTrue(lines[2].startsWith("[FILE] data.csv (456 bytes)"));
    }

    @Test
    void listFiles_includesDotDirectoriesAndFiles() {
        when(filesystem.ls(RT, "/project"))
                .thenReturn(
                        LsResult.success(
                                List.of(
                                        FileInfo.ofDir(".config", "2025-01-01"),
                                        FileInfo.ofDir(".git", "2025-01-01"),
                                        FileInfo.ofDir("src", "2025-01-01"),
                                        FileInfo.ofFile(".gitignore", 42L, "2025-01-01"),
                                        FileInfo.ofFile("pom.xml", 2048L, "2025-01-01"))));

        String result = tool.listFiles(RT, "/project");

        String[] lines = result.split("\n");
        assertEquals(5, lines.length);
        assertTrue(lines[0].startsWith("[DIR]  .config"));
        assertTrue(lines[1].startsWith("[DIR]  .git"));
        assertTrue(lines[2].startsWith("[DIR]  src"));
        assertTrue(lines[3].startsWith("[FILE] .gitignore (42 bytes)"));
        assertTrue(lines[4].startsWith("[FILE] pom.xml (2048 bytes)"));
    }

    @Test
    void listFiles_lsReturnsFailure_returnsErrorMessage() {
        when(filesystem.ls(RT, "/forbidden")).thenReturn(LsResult.fail("Permission denied"));

        String result = tool.listFiles(RT, "/forbidden");

        assertEquals("Error: Permission denied", result);
    }

    @Test
    void listFiles_lsReturnsNullEntries_returnsEmptyDirectory() {
        when(filesystem.ls(RT, "/weird")).thenReturn(new LsResult(null, null));

        String result = tool.listFiles(RT, "/weird");

        assertEquals("Empty directory: /weird", result);
    }

    @Test
    void listFiles_directoryWithOnlySubdirs_stillReturnsEntries() {
        when(filesystem.ls(RT, "/only-dirs"))
                .thenReturn(
                        LsResult.success(
                                List.of(
                                        FileInfo.ofDir("references", "2025-01-01"),
                                        FileInfo.ofDir("scripts", "2025-01-01"),
                                        FileInfo.ofDir("tests", "2025-01-01"))));

        String result = tool.listFiles(RT, "/only-dirs");

        String[] lines = result.split("\n");
        assertEquals(3, lines.length);
        assertTrue(lines[0].startsWith("[DIR]  references"));
        assertTrue(lines[1].startsWith("[DIR]  scripts"));
        assertTrue(lines[2].startsWith("[DIR]  tests"));
    }

    @Test
    void listFiles_relativePath_stillNormalized() {
        WorkspacePathNormalizer normalizer = WorkspacePathNormalizer.of("/workspace");
        tool = new FilesystemTool(filesystem, normalizer);

        when(filesystem.ls(RT, "subdir")).thenReturn(LsResult.success(List.of()));

        tool.listFiles(RT, "subdir");

        // 相对路径跳过 isAbsolutePath 检查, 仍被 normalizer 归一化
        verify(filesystem).ls(RT, "subdir");
    }

    @Test
    void listFiles_normalizerPrefixMismatch_pathUnchanged() {
        WorkspacePathNormalizer normalizer = WorkspacePathNormalizer.of("/workspace");
        tool = new FilesystemTool(filesystem, normalizer);

        when(filesystem.ls(RT, "/other/path")).thenReturn(LsResult.success(List.of()));

        tool.listFiles(RT, "/other/path");

        // 绝对路径但前缀不匹配: isAbsolutePath 为 true → 跳过 normalizer → 原样传递
        verify(filesystem).ls(RT, "/other/path");
    }

    @Test
    void listFiles_passesNormalizedPathToFilesystem() {
        tool = new FilesystemTool(filesystem);

        when(filesystem.ls(RT, "/some/path")).thenReturn(LsResult.success(List.of()));

        tool.listFiles(RT, "/some/path");

        verify(filesystem).ls(RT, "/some/path");
    }

    // 原有测试 ———————————————————————————————————————————————————————

    @Test
    void editFile_omittedReplaceAll_defaultsToFalse() {
        when(filesystem.edit(eq(RT), eq("f.txt"), eq("old"), eq("new"), eq(false)))
                .thenReturn(EditResult.ok("f.txt", 1));

        String result = tool.editFile(RT, "f.txt", "old", "new", null);

        assertTrue(result.startsWith("Edited "));
        verify(filesystem).edit(RT, "f.txt", "old", "new", false);
    }

    @Test
    void editFile_replaceAllTrue_passesTrueToFilesystem() {
        when(filesystem.edit(eq(RT), eq("f.txt"), eq("old"), eq("new"), eq(true)))
                .thenReturn(EditResult.ok("f.txt", 2));

        String result = tool.editFile(RT, "f.txt", "old", "new", true);

        assertTrue(result.contains("2 replacement"));
        verify(filesystem).edit(RT, "f.txt", "old", "new", true);
    }

    @Test
    void listFiles_absolutePath_skipsNormalizer() {
        WorkspacePathNormalizer normalizer =
                WorkspacePathNormalizer.of(
                        "D:\\workspace\\my-learn\\agentscope-v2\\.agentscope\\workspace");
        tool = new FilesystemTool(filesystem, normalizer);
        String agentPath = "D:\\workspace\\my-learn\\agentscope-v2\\.agentscope\\workspace\\memory";

        when(filesystem.ls(RT, agentPath))
                .thenReturn(LsResult.success(List.of(FileInfo.ofDir("memory", ""))));

        String result = tool.listFiles(RT, agentPath);

        assertTrue(result.contains("[DIR]"));
        // 绝对路径不再被 normalizer 剥离, 直接透传至 filesystem
        verify(filesystem).ls(RT, agentPath);
    }

    // ==================== Bug reproduction: listFiles ambiguous error message ====================

    @Test
    void listFiles_nonExistentPath_returnsError() {
        when(filesystem.ls(RT, "/nonexistent"))
                .thenReturn(LsResult.fail("Path does not exist: /nonexistent"));

        String result = tool.listFiles(RT, "/nonexistent");

        assertTrue(result.startsWith("Error:"), "should report error for non-existent path");
        assertTrue(result.contains("does not exist"), "error should mention 'does not exist'");
    }

    @Test
    void listFiles_filePath_returnsError() {
        when(filesystem.ls(RT, "/path/to/file.txt"))
                .thenReturn(LsResult.fail("Not a directory: /path/to/file.txt"));

        String result = tool.listFiles(RT, "/path/to/file.txt");

        assertTrue(result.startsWith("Error:"), "should report error for file path");
        assertTrue(result.contains("Not a directory"), "error should mention 'Not a directory'");
    }

    @Test
    void listFiles_emptyDirectory_returnsEmptyDirMessage() {
        when(filesystem.ls(RT, "/empty/dir")).thenReturn(LsResult.success(List.of()));

        String result = tool.listFiles(RT, "/empty/dir");

        assertEquals("Empty directory: /empty/dir", result);
    }

    // ==================== Bug reproduction: listFiles ambiguous error message ====================

    @Test
    void listFiles_nonExistentPath_returnsError() {
        when(filesystem.ls(RT, "/nonexistent"))
                .thenReturn(LsResult.fail("Path does not exist: /nonexistent"));

        String result = tool.listFiles(RT, "/nonexistent");

        assertTrue(result.startsWith("Error:"), "should report error for non-existent path");
        assertTrue(result.contains("does not exist"), "error should mention 'does not exist'");
    }

    @Test
    void listFiles_filePath_returnsError() {
        when(filesystem.ls(RT, "/path/to/file.txt"))
                .thenReturn(LsResult.fail("Not a directory: /path/to/file.txt"));

        String result = tool.listFiles(RT, "/path/to/file.txt");

        assertTrue(result.startsWith("Error:"), "should report error for file path");
        assertTrue(result.contains("Not a directory"), "error should mention 'Not a directory'");
    }

    @Test
    void listFiles_emptyDirectory_returnsEmptyDirMessage() {
        when(filesystem.ls(RT, "/empty/dir")).thenReturn(LsResult.success(List.of()));

        String result = tool.listFiles(RT, "/empty/dir");

        assertEquals("Empty directory: /empty/dir", result);
    }

    @Test
    void readFile_omittedOffsetAndLimit_defaultToZero() {
        when(filesystem.read(eq(RT), eq("f.txt"), eq(0), eq(0)))
                .thenReturn(ReadResult.success(new FileData("hello", "utf-8")));

        String result = tool.readFile(RT, "f.txt", null, null);

        assertEquals("hello", result);
        verify(filesystem).read(RT, "f.txt", 0, 0);
    }

    @Test
    void readFile_explicitOffsetAndLimit_arePassedThrough() {
        when(filesystem.read(eq(RT), eq("f.txt"), eq(2), eq(5)))
                .thenReturn(ReadResult.success(new FileData("world", "utf-8")));

        String result = tool.readFile(RT, "f.txt", 2, 5);

        assertEquals("world", result);
        verify(filesystem).read(RT, "f.txt", 2, 5);
    }

    // ---------------------------------------------------------------
    // listFiles 集成测试 — 使用真实 LocalFilesystem 后端
    // ---------------------------------------------------------------

    @Test
    void listFiles_withLocalFilesystem_directoryWithOnlySubdirs_succeeds(@TempDir Path tempDir)
            throws Exception {
        // 模拟用户场景: office-tools-ppt/office-tools-ppt 两级嵌套，最内层仅有目录
        Path outer = tempDir.resolve("office-tools-ppt");
        Path inner = outer.resolve("office-tools-ppt");
        Files.createDirectories(inner.resolve("references"));
        Files.createDirectories(inner.resolve("scripts"));
        Files.createDirectories(inner.resolve("tests"));

        LocalFilesystem fs = new LocalFilesystem(tempDir);
        FilesystemTool tool = new FilesystemTool(fs);

        // 测试: 外层 office-tools-ppt 下只有 office-tools-ppt/ 一个子目录（无文件）
        String outerResult = tool.listFiles(RT, "office-tools-ppt");
        // 先直接断言完整输出，方便看清实际返回
        assertTrue(outerResult.contains("[DIR]"), "外层仅有子目录时应正常列出，但实际返回: [" + outerResult + "]");

        // 测试: 内层 office-tools-ppt/office-tools-ppt 下仅有 references, scripts, tests（无文件）
        String innerResult = tool.listFiles(RT, "office-tools-ppt/office-tools-ppt");
        assertTrue(innerResult.contains("[DIR]"), "内层仅有子目录时应正常列出，但实际返回: [" + innerResult + "]");
    }

    @Test
    void listFiles_withLocalFilesystem_nestedDotDirectory_onlyDirs(@TempDir Path tempDir)
            throws Exception {
        Path deep =
                tempDir.resolve(".agentscope")
                        .resolve("workspace")
                        .resolve("TEST-ID")
                        .resolve(".skills-cache")
                        .resolve("git-copilot-plugins")
                        .resolve("skills")
                        .resolve("default")
                        .resolve("office-tools-ppt")
                        .resolve("office-tools-ppt");
        Files.createDirectories(deep.resolve("references"));
        Files.createDirectories(deep.resolve("scripts"));
        Files.createDirectories(deep.resolve("tests"));

        LocalFilesystem fs = new LocalFilesystem(tempDir);
        FilesystemTool tool = new FilesystemTool(fs);

        String result =
                tool.listFiles(
                        RT,
                        ".agentscope/workspace/TEST-ID/.skills-cache/git-copilot-plugins/skills/default/office-tools-ppt/office-tools-ppt");

        assertTrue(result.contains("[DIR]"), "隐藏路径 + 仅有目录时应正常列出，但实际返回: [" + result + "]");
    }

    @Test
    void listFiles_withLocalFilesystem_andPathNormalizer_onlyDirs(@TempDir Path tempDir)
            throws Exception {
        Path workspaceRoot = tempDir.resolve(".agentscope").resolve("workspace").resolve("TEST-ID");
        Path deep =
                workspaceRoot
                        .resolve(".skills-cache")
                        .resolve("git-copilot-plugins")
                        .resolve("skills")
                        .resolve("default")
                        .resolve("office-tools-ppt")
                        .resolve("office-tools-ppt");
        Files.createDirectories(deep.resolve("references"));
        Files.createDirectories(deep.resolve("scripts"));
        Files.createDirectories(deep.resolve("tests"));

        // 实际 HarnessAgent: cwd = 工作区根目录, normalizer 前缀 = 同一目录
        LocalFilesystem fs = new LocalFilesystem(workspaceRoot);
        WorkspacePathNormalizer normalizer = WorkspacePathNormalizer.of(workspaceRoot.toString());
        FilesystemTool tool = new FilesystemTool(fs, normalizer);

        String agentPath =
                workspaceRoot
                        .resolve(
                                ".skills-cache/git-copilot-plugins/skills/default/office-tools-ppt/office-tools-ppt")
                        .toString();

        String result = tool.listFiles(RT, agentPath);

        assertTrue(result.contains("[DIR]"), "normalizer 剥离前缀后应正常列出，但实际返回: [" + result + "]");
    }

    @Test
    void grepFiles_omittedLimit_appliesServerDefaultAndReportsTruncation() {
        when(filesystem.grep(RT, "needle", ".", null))
                .thenReturn(GrepResult.success(grepMatches(FilesystemTool.DEFAULT_GREP_LIMIT + 1)));

        String result = tool.grepFiles(RT, "needle", ".", null, null);

        assertTrue(result.contains("file-99.txt:100:match-99"));
        assertFalse(result.contains("file-100.txt:101:match-100"));
        assertTrue(result.contains("showing 100 of 101 matches"));
    }

    @Test
    void grepFiles_explicitLimit_isApplied() {
        when(filesystem.grep(RT, "needle", ".", null))
                .thenReturn(GrepResult.success(grepMatches(3)));

        String result = tool.grepFiles(RT, "needle", ".", null, 2);

        assertTrue(result.contains("file-1.txt:2:match-1"));
        assertFalse(result.contains("file-2.txt:3:match-2"));
        assertTrue(result.contains("showing 2 of 3 matches"));
    }

    @Test
    void grepFiles_limitAboveMaximum_isCapped() {
        when(filesystem.grep(RT, "needle", ".", null))
                .thenReturn(GrepResult.success(grepMatches(FilesystemTool.MAX_SEARCH_LIMIT + 1)));

        String result = tool.grepFiles(RT, "needle", ".", null, Integer.MAX_VALUE);

        assertFalse(result.contains("file-1000.txt:1001:match-1000"));
        assertTrue(result.contains("showing 1000 of 1001 matches"));
        assertTrue(result.contains("Hard maximum of 1000 reached"));
        assertFalse(result.contains("increase limit"));
    }

    @Test
    void grepFiles_nonPositiveLimit_isRejectedBeforeSearch() {
        String result = tool.grepFiles(RT, "needle", ".", null, 0);

        assertEquals("Error: limit must be greater than 0", result);
        verifyNoInteractions(filesystem);
    }

    @Test
    void globFiles_omittedLimit_appliesServerDefaultAndReportsTruncation() {
        when(filesystem.glob(RT, "**/*.txt", "."))
                .thenReturn(GlobResult.success(files(FilesystemTool.DEFAULT_GLOB_LIMIT + 1)));

        String result = tool.globFiles(RT, "**/*.txt", ".", null);

        assertTrue(result.contains("file-199.txt (199 bytes)"));
        assertFalse(result.contains("file-200.txt (200 bytes)"));
        assertTrue(result.contains("showing 200 of 201 files"));
    }

    @Test
    void searchOverloads_remainBackwardCompatibleForDirectCallers() {
        when(filesystem.grep(RT, "needle", ".", null))
                .thenReturn(GrepResult.success(grepMatches(1)));
        when(filesystem.glob(RT, "*.txt", ".")).thenReturn(GlobResult.success(files(1)));

        assertEquals("file-0.txt:1:match-0", tool.grepFiles(RT, "needle", ".", null));
        assertEquals("file-0.txt (0 bytes)", tool.globFiles(RT, "*.txt", "."));
    }

    @Test
    @SuppressWarnings("unchecked")
    void searchToolSchemas_exposeLimitAsOptional() {
        Toolkit toolkit = new Toolkit();
        toolkit.registerTool(tool);

        for (String toolName : List.of("grep_files", "glob_files")) {
            AgentTool registered = toolkit.getTool(toolName);
            Map<String, Object> schema = registered.getParameters();
            Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
            List<String> required = (List<String>) schema.get("required");

            assertTrue(properties.containsKey("limit"));
            assertFalse(required.contains("limit"));
        }
    }

    private static List<GrepMatch> grepMatches(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new GrepMatch("file-" + i + ".txt", i + 1, "match-" + i))
                .toList();
    }

    private static List<FileInfo> files(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> FileInfo.ofFile("file-" + i + ".txt", i, ""))
                .toList();
    }
}
