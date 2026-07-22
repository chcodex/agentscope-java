## 构建与测试指令 (Build and Test Commands)

### 根项目 (Maven)
- 构建: `mvn clean compile`
- 全量测试: `mvn test`
- 单测执行: `mvn test -Dtest=ClassName` (替换为实际类名)

### 子模块与子项目 (Sub-modules & Sub-projects)
- agentscope-examples: Maven 模块（Java 17）。受根项目指令统一控制。
### 模块 (Module): docs (Python)
- 环境: 优先检查并激活 `venv` 或 `.venv`
- 依赖: `cd docs && pip install -r requirements.txt`
- 全量测试: `cd docs && pytest`
- 单文件测试: `cd docs && pytest path/to/test_file.py` (替换为实际路径)

- agentscope-harness: Maven 模块。受根项目指令统一控制。
- agentscope-extensions: Maven 模块。受根项目指令统一控制。
- agentscope-distribution: Maven 模块。受根项目指令统一控制。
- agentscope-dependencies-bom: Maven 模块（Java 17）。受根项目指令统一控制。
- agentscope-core: Maven 模块（Java 17）。受根项目指令统一控制。

## 环境版本 (Environment Versions)

> 以下为项目配置文件中**声明**的版本（非本机安装版本），写代码时请对齐该版本的语法特性。

- Maven (Root): Java 17
- docs (Python): Python >=3.10

## 工程规约 (Guidelines)

- **读前必改**: 在进行任何修改前，务必完整阅读相关文件内容。
- **原子作业**: 每次仅实现一个功能或修复一个 Bug。
- **验证驱动**: 任务完成前必须运行测试进行验证。
- **路径规范**: 仅使用相对路径（例如：`src/main/java/App.java`，严禁使用 `./src/...`）。
- **风格对齐**: 必须遵循代码库中已有的编码风格和设计模式。
- **版本对齐**: 参考「环境版本」章节声明的版本，不要使用超过该版本的语法特性；若未列出版本，应从配置文件或构建工具复核后再决定。
- **环境感知**: 利用你对各语言默认本地仓库路径（如 Maven、Node）的知识，协助排查依赖问题或进行源码分析。

