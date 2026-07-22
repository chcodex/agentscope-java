## 补充 `E2bEnvdProcessClientTest.java` 覆盖缺口

### 第一阶段：PROTO codec + drainStartStream 边界
- [x] PROTO codec drainStartStream 冒烟测试（数据帧+结束流）
- [x] 非法帧长度 (len > 64MB) 抛 IOException
- [x] 截断帧数据 (data.length < len) 跳出循环

### 第二阶段：parseJsonStartResponse 边界
- [x] 非整数 exitCode 路径（如 `"exitCode": "abc"`）
- [x] 非文本 error 路径（如 `"error": 123`）
- [x] 空数据帧（len=0）跳过

### 第三阶段：静态工具方法
- [x] filesystemHost() 自定义 domain
- [x] filesystemHost() 默认 domain
- [x] basicAuthUser(null) 返回默认 "Basic dXNlcjo="
- [x] basicAuthUser("admin") 自定义用户名

### 第四阶段：验证
- [x] 运行测试验证全部通过（26/26 ✅）
