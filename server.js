
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { existsSync } from "node:fs";
import { unlink } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  disposeRuntime,
  getCurrentCwd,
  getCurrentMessages,
  getCurrentSession,
  getCurrentSessionFile,
  getRuntime,
  isInitialized,
  isStreaming,
  renameSessionFile,
  subscribeSession,
  switchToSession,
} from "./pi-runtime.js";
import {
  assertSessionPathInScope,
  forgetSession,
  listSessionGroups,
  resolveSession,
  samePath,
} from "./session-index.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT || 3000);

const WORKSPACE = path.resolve(
  process.env.PI_WORKSPACE || path.resolve(__dirname, "..")
);

app.use(cors());
app.use(express.json({ limit: "2mb" }));

// ============================================================
// 会话状态
// ============================================================
//
// 会话本身由 pi-runtime.js 持有，这里只留两个锁：
// isPrompting —— 有 prompt 在跑；isSwitching —— 正在重建运行时。
// 两者互斥：任一个为真，另一边的请求都要被拒绝。

let isPrompting = false;
let isSwitching = false;

// ============================================================
// SSE 工具
// ============================================================

function setupSSE(res) {
  res.status(200);
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");

  res.flushHeaders?.();

  // 通知浏览器 SSE 连接已经建立
  res.write(": connected\n\n");
}

function sendSSE(res, eventName, data) {
  if (res.destroyed || res.writableEnded) {
    return false;
  }

  try {
    res.write(`event: ${eventName}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
    return true;
  } catch (error) {
    console.error("[SSE] 发送失败：", error.message);
    return false;
  }
}

// ============================================================
// 健康检查
// ============================================================

app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    message: "Pi Agent Web 后端运行正常",
    workspace: getCurrentCwd() ?? WORKSPACE,
    sessionInitialized: isInitialized(),
    isPrompting,
  });
});

// ============================================================
// 会话历史：两级树
// ============================================================

app.get("/api/sessions", async (req, res) => {
  try {
    // 注意这里不调 getRuntime()：只是看一眼列表，不应该把运行时创建起来。
    const currentCwd = getCurrentCwd() ?? WORKSPACE;
    const groups = await listSessionGroups(currentCwd);
    const session = getCurrentSession();

    res.json({
      success: true,
      currentCwd,
      // 当前会话正在用的文件。新会话还没落盘时这个路径尚不存在，
      // 前端按路径匹配自然匹配不到任何节点（设计 7.2）。
      activeSessionPath: session?.sessionFile ?? null,
      groups,
    });
  } catch (error) {
    console.error("[会话] 列举失败：", error);
    res.status(500).json({
      success: false,
      code: "LIST_FAILED",
      error: error instanceof Error ? error.message : "读取会话列表失败",
    });
  }
});

// ============================================================
// 会话历史：切换
// ============================================================

/**
 * 有生成在进行中。
 *
 * 两个来源都要看：`isPrompting` 是 /api/chat 自己的标志，只覆盖走这个接口的生成；
 * `isStreaming()` 是会话的真实状态，能兜住「HTTP 请求已断但模型还在跑」。
 */
function isGenerating() {
  return isPrompting || isStreaming();
}

app.post("/api/sessions/switch", async (req, res) => {
  const id = typeof req.body?.id === "string" ? req.body.id.trim() : "";

  if (!id) {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "缺少会话 id",
    });
  }

  if (isGenerating()) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "当前会话正在生成，无法切换。",
      currentCwd: getCurrentCwd() ?? WORKSPACE,
    });
  }

  if (isSwitching) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "正在切换会话，请稍后再试。",
      currentCwd: getCurrentCwd() ?? WORKSPACE,
    });
  }

  // 从这一刻到 finally 之间，运行时处于「正在被拆掉重建」的状态。
  // 期间必须挡住并发的切换和生成：两个 switchSession 同时跑会各自建一套运行时，
  // 后写的那个把先写的那个覆盖掉，先写的那套会话不会被 dispose，文件句柄就漏了。
  isSwitching = true;

  try {
    const target = await resolveSession(id);

    if (!target) {
      return res.status(404).json({
        success: false,
        code: "NOT_FOUND",
        error: "找不到该会话，它可能已被删除。",
      });
    }

    // 提前挡掉不可用的工作目录。
    // unknown 这一类单靠切会话是拦不住的 —— Pi 不认为空 cwd 有问题，
    // 会静默把进程当前目录当工作目录用（设计 7.5）。
    if (target.cwdStatus === "unknown") {
      return res.status(409).json({
        success: false,
        code: "CWD_MISSING",
        reason: "not-recorded",
        sessionCwd: target.cwd,
        currentCwd: getCurrentCwd() ?? WORKSPACE,
        error: "这个会话没有记录工作目录，无法切换。",
      });
    }

    if (target.cwdStatus === "missing") {
      return res.status(409).json({
        success: false,
        code: "CWD_MISSING",
        reason: "not-exists",
        sessionCwd: target.cwd,
        currentCwd: getCurrentCwd() ?? WORKSPACE,
        error: `该会话的工作目录已不存在：${target.cwd}`,
      });
    }

    const result = await switchToSession(target.path);

    if (result?.cancelled) {
      return res.status(409).json({
        success: false,
        code: "CANCELLED",
        error: "切换被扩展取消。",
      });
    }

    const session = getCurrentSession();

    console.log(`[会话] 已切换：${target.cwd}`);

    return res.json({
      success: true,
      currentCwd: getCurrentCwd() ?? WORKSPACE,
      activeSessionPath: session?.sessionFile ?? null,
      // 聊天区要换成新会话的内容。放在这个响应里而不是再开一个接口：
      // 切换本来就已经有一次重新列举的往返了，没必要再多一次（设计 第十节）。
      messages: getCurrentMessages(),
    });
  } catch (error) {
    // 上面的 cwdStatus 预检到真正切换之间有窗口期（TOCTOU），
    // 目录可能刚好被删掉，这里兜底。
    if (error?.name === "MissingSessionCwdError") {
      return res.status(409).json({
        success: false,
        code: "CWD_MISSING",
        reason: "not-exists",
        sessionCwd: error.issue?.sessionCwd ?? null,
        currentCwd: getCurrentCwd() ?? WORKSPACE,
        error: `该会话的工作目录已不存在：${error.issue?.sessionCwd ?? "（未知）"}`,
      });
    }

    console.error("[会话] 切换失败：", error);

    return res.status(500).json({
      success: false,
      code: "SWITCH_FAILED",
      error: error instanceof Error ? error.message : "切换会话失败",
    });
  } finally {
    isSwitching = false;
  }
});

// ============================================================
// 会话历史：重命名 / 删除
// ============================================================

/** 名字长度上限。树里只显示一行，再长的名字也没有意义。 */
const MAX_SESSION_NAME = 120;

/**
 * 名字清洗，与 SDK 的 appendSessionInfo 保持一致：
 * 换行清成空格再 trim。空串是合法的 —— 表示清除名字，标题退回第一条用户消息。
 */
function sanitizeSessionName(raw) {
  return raw.replace(/[\r\n]+/g, " ").trim();
}

/**
 * 两个写接口共用的前置：把 id 解析成路径，并确认它确实可以动。
 *
 * 检查顺序不是随意的：
 *   1. 解析 id —— 找不到就 404，这一步之后就绝不再碰调用方给的任何字符串
 *   2. 范围断言（设计 9.2）—— **任何文件系统操作之前**，把路径锁死在会话目录内
 *   3. 文件还在不在 —— SessionManager.open() 遇到不存在的路径会悄悄新建一个空会话，
 *      必须在这之前挡掉，否则"重命名一个已被删掉的会话"会凭空造出一个文件
 *
 * 检查不通过时直接回响应并返回 null，调用方据此收工。
 *
 * @returns {Promise<{target: object, sessionPath: string} | null>}
 */
async function prepareSessionWrite(res, id) {
  const target = await resolveSession(id);

  if (!target) {
    res.status(404).json({
      success: false,
      code: "NOT_FOUND",
      error: "找不到该会话，它可能已被删除。",
    });
    return null;
  }

  let sessionPath;

  try {
    sessionPath = assertSessionPathInScope(target.path);
  } catch (error) {
    // 正常情况下到不了这里：路径一律来自 SessionManager 的列举结果。
    // 真到了这里说明"路径在会话目录内"这个前提不成立，宁可拒绝也不动文件。
    console.error("[会话] 路径超出会话目录范围：", target.path);

    res.status(400).json({
      success: false,
      code: "OUT_OF_SCOPE",
      error: error.message,
    });
    return null;
  }

  if (!existsSync(sessionPath)) {
    forgetSession(id);

    res.status(404).json({
      success: false,
      code: "NOT_FOUND",
      error: "会话文件已不存在，它可能已被删除。",
    });
    return null;
  }

  return { target, sessionPath };
}

/**
 * 切换进行中拒绝这两个写操作。
 *
 * 不为别的，只为 `getCurrentSession()` 在运行时被拆掉重建的那一瞬是不可靠的 ——
 * 而 IS_ACTIVE 判断（不能删当前会话）恰好依赖它。窗口很短，等一下就行。
 */
function rejectWhileSwitching(res) {
  if (!isSwitching) {
    return false;
  }

  res.status(409).json({
    success: false,
    code: "BUSY",
    error: "正在切换会话，请稍后再试。",
  });

  return true;
}

/**
 * PATCH /api/sessions/:id —— 重命名。
 *
 * 生成期间**不拦**：这只是往文件末尾追加一条 session_info，改的是元数据，
 * 不碰消息本身。当前会话也走这条（用运行时手里那个 SessionManager），
 * 每次追加都是一次完整的同步写，不会和模型正在写的消息交错成半个 JSON 行。
 * 唯一拦的是切换中的那一瞬，理由见 rejectWhileSwitching。
 */
app.patch("/api/sessions/:id", async (req, res) => {
  const id = typeof req.params.id === "string" ? req.params.id.trim() : "";
  const rawName = req.body?.name;

  if (!id) {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "缺少会话 id",
    });
  }

  if (typeof rawName !== "string") {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "name 必须是字符串",
    });
  }

  const name = sanitizeSessionName(rawName);

  if (name.length > MAX_SESSION_NAME) {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: `名字过长，最多 ${MAX_SESSION_NAME} 个字符。`,
    });
  }

  if (rejectWhileSwitching(res)) {
    return;
  }

  try {
    const prepared = await prepareSessionWrite(res, id);

    if (!prepared) {
      return;
    }

    renameSessionFile(prepared.sessionPath, name);

    console.log(`[会话] 已重命名：${name || "（清除名字）"}`);

    return res.json({ success: true, id, name });
  } catch (error) {
    console.error("[会话] 重命名失败：", error);

    return res.status(500).json({
      success: false,
      code: "RENAME_FAILED",
      error: error instanceof Error ? error.message : "重命名会话失败",
    });
  }
});

/**
 * DELETE /api/sessions/:id —— 删除。
 *
 * 当前会话独立拒绝（设计 7.3）：runtime 还指着这个文件，删掉之后下一条消息
 * 会把它重新创建出来，变成"删了又回来"；Windows 上删除被打开的文件还可能 EBUSY。
 * 所以想删当前会话，得先切到同目录下别的会话。
 *
 * 工作目录不可用**不影响**删除 —— 门口那些"目录已删除"的会话恰恰是最该清理的，
 * 而且删文件只用得到路径，跟 cwd 在不在没关系。
 */
app.delete("/api/sessions/:id", async (req, res) => {
  const id = typeof req.params.id === "string" ? req.params.id.trim() : "";

  if (!id) {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "缺少会话 id",
    });
  }

  if (rejectWhileSwitching(res)) {
    return;
  }

  try {
    const prepared = await prepareSessionWrite(res, id);

    if (!prepared) {
      return;
    }

    const activePath = getCurrentSessionFile();

    if (activePath && samePath(activePath, prepared.sessionPath)) {
      return res.status(409).json({
        success: false,
        code: "IS_ACTIVE",
        error: "不能删除当前正在使用的会话，先切到同目录下的其它会话再删。",
      });
    }

    await unlink(prepared.sessionPath);

    // 缓存里那个条目现在指向一个不存在的文件，立刻摘掉。
    forgetSession(id);

    console.log(`[会话] 已删除：${path.basename(prepared.sessionPath)}`);

    return res.json({ success: true, id });
  } catch (error) {
    if (error?.code === "ENOENT") {
      forgetSession(id);

      return res.status(404).json({
        success: false,
        code: "NOT_FOUND",
        error: "会话文件已不存在，它可能已被删除。",
      });
    }

    // Windows 上文件被别的进程（终端里的 pi）打开时是这两个码。
    if (error?.code === "EBUSY" || error?.code === "EPERM" || error?.code === "EACCES") {
      return res.status(409).json({
        success: false,
        code: "FILE_BUSY",
        error: "这个会话文件正被其它程序占用，删除失败。",
      });
    }

    console.error("[会话] 删除失败：", error);

    return res.status(500).json({
      success: false,
      code: "DELETE_FAILED",
      error: error instanceof Error ? error.message : "删除会话失败",
    });
  }
});

// ============================================================
// 聊天接口：真正的增量流式输出
// ============================================================

app.post("/api/chat", async (req, res) => {
  const message =
    typeof req.body?.message === "string"
      ? req.body.message.trim()
      : "";

  // 参数校验在建立 SSE 连接之前完成
  if (!message) {
    return res.status(400).json({
      success: false,
      error: "消息内容不能为空",
    });
  }

  if (isPrompting) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "当前已有消息正在生成，请稍后再试。",
    });
  }

  // 正在切换会话时不许开始生成：会话马上会被整个替换掉，
  // 这时候 prompt 打在旧会话上，轻则报错重则打到已 dispose 的会话上。
  if (isSwitching) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "正在切换会话，请稍后再试。",
    });
  }

  // 同一 Session 一次只执行一个 prompt
  isPrompting = true;

  let unsubscribe = null;
  let clientDisconnected = false;

  const requestId = Math.random().toString(36).slice(2, 10);

  const onClose = () => {
    if (!res.writableEnded) {
      clientDisconnected = true;
      console.log(`[${requestId}] 客户端连接已断开`);
    }
  };

  res.on("close", onClose);

  try {
    setupSSE(res);

    console.log(`[${requestId}] 用户消息：${message}`);

    const runtime = await getRuntime(WORKSPACE);
    const session = runtime.session;

    if (clientDisconnected) {
      return;
    }

    // 订阅模型事件，在每个文本增量到达时立即发送。
    // 用 subscribeSession 而不是 session.subscribe：会话在请求期间被替换时，
    // 订阅会自动接到新会话上，不会断。
    unsubscribe = subscribeSession((event) => {
      if (
        event.type !== "message_update" ||
        event.assistantMessageEvent?.type !== "text_delta"
      ) {
        return;
      }

      const delta = event.assistantMessageEvent.delta;

      if (typeof delta !== "string" || delta.length === 0) {
        return;
      }

      if (!clientDisconnected) {
        sendSSE(res, "message", { text: delta });
      }
    });

    // 不等待整个回答后再读取，而是在执行期间由事件回调推送文本
    await session.prompt(message);

    if (!clientDisconnected) {
      sendSSE(res, "done", {
        success: true,
        requestId,
      });
    }

    console.log(`[${requestId}] 回复完成`);
  } catch (error) {
    console.error(`[${requestId}] 聊天请求失败：`, error);

    if (!clientDisconnected) {
      sendSSE(res, "error", {
        success: false,
        error:
          error instanceof Error
            ? error.message
            : "Pi Agent 执行失败",
        requestId,
      });
    }
  } finally {
    if (typeof unsubscribe === "function") {
      try {
        unsubscribe();
      } catch (error) {
        console.error("[Pi] 取消事件订阅失败：", error);
      }
    }

    res.off("close", onClose);

    isPrompting = false;

    if (!res.destroyed && !res.writableEnded) {
      res.end();
    }
  }
});

// ============================================================
// 404 与全局错误处理
// ============================================================

app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: `接口不存在：${req.method} ${req.originalUrl}`,
  });
});

app.use((error, req, res, next) => {
  console.error("[Express] 未处理的错误：", error);

  if (res.headersSent) {
    return next(error);
  }

  res.status(500).json({
    success: false,
    error: error instanceof Error ? error.message : String(error),
  });
});

// ============================================================
// 启动服务
// ============================================================

const server = app.listen(PORT, "127.0.0.1", () => {
  console.log("========================================");
  console.log(" Pi Agent Web 后端");
  console.log("========================================");
  console.log(`[服务] http://127.0.0.1:${PORT}`);
  console.log(`[健康检查] http://127.0.0.1:${PORT}/api/health`);
  console.log(`[工作目录] ${WORKSPACE}`);
});

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  console.log(`\n[服务] 收到 ${signal}，正在关闭...`);

  // 先关运行时，让 Pi 侧正常收尾（扩展收到 session_shutdown、会话落盘），
  // 再关闭 HTTP 服务。
  try {
    await disposeRuntime();
  } catch (error) {
    console.error("[Pi] 关闭运行时失败：", error);
  }

  server.close(() => {
    process.exit(0);
  });
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
