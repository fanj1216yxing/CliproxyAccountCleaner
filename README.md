# CliproxyAccountCleaner

基于 CLIProxy 管理端的账号巡检与批量处理工具。  
当前支持桌面模式（Tk）和网页模式（推荐），并已支持 Docker 部署。

## 1. 当前能力概览

### 1.1 检测能力
- 401 无效检测
- 额度检测（周额度 + 5 小时额度阈值）
- 联合检测（401 + 额度）

### 1.2 批量动作
- 关闭选中账号
- 恢复已关闭账号
- 永久删除账号
- 加入备用池
- 备用转活跃（先检测后开启）

### 1.3 自动巡检
- 按间隔循环执行检测
- 可配置 401 账号处理策略（删除/仅标记）
- 可配置额度耗尽处理策略（关闭/删除/仅标记）
- 可配置活跃账号目标数（超出回收，不足补齐）
- 补齐顺序：备用池优先，可选从已关闭池补齐

### 1.4 网页登录与会话
- 新增登录页（账号密码）
- 登录凭据从 `config.json` 读取，不再硬编码
- 会话 Cookie（HttpOnly + SameSite=Lax）
- 支持退出登录
- 配置字段缺失时自动补齐为免登录（默认登录态）

## 2. 目录结构

```text
CliproxyAccountCleaner.py     # 主入口（默认网页模式）
cliproxy_web_mode.py          # 网页端逻辑（单文件）
config.json                   # 运行配置（含登录配置）
Dockerfile                    # Docker 镜像构建
docker-compose.yml            # Docker Compose 启动
.dockerignore
```

## 3. 快速开始（本地）

### 3.1 依赖

- Python 3.10+
- `requests`
- `aiohttp`

安装：

```bash
pip install requests aiohttp
```

### 3.2 启动网页模式（推荐）

```bash
python CliproxyAccountCleaner.py --host 127.0.0.1 --port 8765 --no-browser
```

浏览器访问：

```text
http://127.0.0.1:8765
```

### 3.3 启动桌面模式（可选）

```bash
python CliproxyAccountCleaner.py --desktop
```

注意：桌面模式需要 `tkinter` 运行时。无 `tkinter` 环境下请使用网页模式。

## 4. Docker 部署

## 4.1 使用 `docker compose`（推荐）

```bash
docker compose up -d --build
```

默认映射：
- `8765:8765`

访问：

```text
http://127.0.0.1:8765
```

停止：

```bash
docker compose down
```

## 4.2 纯 `docker` 命令

构建：

```bash
docker build -t cliproxy-account-cleaner:latest .
```

启动：

```bash
docker run -d \
  --name cliproxy-account-cleaner \
  -p 8765:8765 \
  -v %cd%/config.json:/app/config.json \
  -v %cd%/data:/data \
  cliproxy-account-cleaner:latest
```

Linux/macOS 的 volume 路径请改为 `$(pwd)`。

## 5. 配置说明（config.json）

最小可用配置（仅开启网页登录）：

```json
{
  "web_login_username": "admin",
  "web_login_password": "admin"
}
```

常用业务配置示例：

```json
{
  "base_url": "https://your-cpa-host",
  "token": "your-token",
  "target_type": "codex",
  "provider": "",
  "workers": 100,
  "quota_workers": 100,
  "close_workers": 20,
  "enable_workers": 20,
  "delete_workers": 20,
  "timeout": 10,
  "retries": 1,
  "weekly_quota_threshold": 99,
  "primary_quota_threshold": 99,
  "auto_check_interval_minutes": 60,
  "auto_401_action": "delete",
  "auto_quota_action": "close",
  "auto_keep_active_count": 100,
  "auto_allow_closed_scan": true,
  "web_login_username": "admin",
  "web_login_password": "admin"
}
```

说明：
- `token` 与 `cpa_password` 兼容，建议只维护 `token`
- `web_login_username/password` 都为空时，自动降级为免登录
- 若只填一个登录字段，会给出配置提示并降级为免登录

## 6. 自动巡检行为说明

每轮巡检核心流程：

1. 刷新并执行联合检测（401 + 额度）
2. 按策略处理异常账号
3. 若活跃数超出目标：关闭溢出账号并回收到备用池
4. 若活跃数低于目标：先从备用池恢复，再按配置从已关闭池补齐

其中“额度耗尽 -> 移出备用并保持关闭”的含义是：
- 该账号不再走备用池优先补齐路径
- 仅在允许从已关闭池补齐且额度恢复后，才可能再次被启用

## 7. 常见问题

### 7.1 启动报 `config.json 格式错误`
- 先检查 JSON 语法（逗号、引号）
- 当前代码已兼容 UTF-8 BOM；建议仍使用 UTF-8 编码保存

### 7.2 开了自动巡检但没有自动关闭
- 确认 `auto_quota_action` 是否为 `close` 或 `delete`
- 确认你检测阈值设置合理（`weekly_quota_threshold` / `primary_quota_threshold`）
- 先手动执行一次“联合检测”观察状态是否正确识别

### 7.3 登录后立刻掉回登录页
- 会话过期或 Cookie 被拦截，检查浏览器隐私设置
- 确认系统时间正常

## 8. 安全建议

- 不要在公开仓库提交真实 `token`
- 生产环境务必改默认登录密码
- 建议配合反向代理启用 HTTPS

---

仅用于运维与测试场景，请确保使用行为符合平台条款与当地法律法规。

