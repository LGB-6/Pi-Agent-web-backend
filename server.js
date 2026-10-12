
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createAgentSession,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

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
// Pi Session 管理
// ============================================================

let piSession = null;
let sessionPromise = null;
let isPrompting = false;

async function getPiSession() {
  if (piSession) {
    return piSession;
  }

  if (sessionPromise) {
    return sessionPromise;
  }

  sessionPromise = (async () => {
    console.log("[Pi] 正在初始化 Session...");

    const result = await createAgentSession({
      cwd: WORKSPACE,
      sessionManager: SessionManager.create(WORKSPACE),
    });

    // 兼容 SDK 返回整体结果对象或直接返回 Session 的情况
    const session = result.session ?? result;

    if (typeof session.prompt !== "function") {
      throw new Error("Pi SDK 返回对象没有 prompt() 方法");
    }

    if (typeof session.subscribe !== "function") {
      throw new Error("Pi SDK 返回对象没有 subscribe() 方法");
    }

    piSession = session;

    console.log("[Pi] Session 初始化成功");
    console.log("[Pi] 工作目录：", WORKSPACE);

    return session;
  })();

  try {
    return await sessionPromise;
  } finally {
    sessionPromise = null;
  }
}

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
    workspace: WORKSPACE,
    sessionInitialized: Boolean(piSession),
    isPrompting,
  });
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
      error: "当前已有消息正在生成，请稍后再试。",
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

    const session = await getPiSession();

    if (clientDisconnected) {
      return;
    }

    // 订阅模型事件，在每个文本增量到达时立即发送
    unsubscribe = session.subscribe((event) => {
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

function shutdown(signal) {
  console.log(`\n[服务] 收到 ${signal}，正在关闭...`);

  server.close(() => {
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
