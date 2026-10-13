import {
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  getAgentDir,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

import { toChatMessages } from "./chat-messages.js";
import { samePath } from "./session-index.js";

// ============================================================
// Pi 会话运行时持有者
// ============================================================
//
// 与「进程启动时创建一个 AgentSession、之后只读」不同，这里持有的是
// AgentSessionRuntime：它可以在运行期把当前会话整个换掉（switchSession /
// newSession / fork），并在换完之后回调 setRebindSession。
//
// 两个关键约束：
//
// 1. 会话被替换后，旧的 AgentSession 会被 dispose。任何挂在旧会话上的订阅
//    都必须重新挂到新会话上，否则替换之后事件流就断了。这里用 listeners
//    集合统一中转，并在 rebind 时重新挂载 —— 调用方只订阅一次即可。
//
// 2. 调用方不要缓存 runtime.session，会话随时可能被换掉。要么每次调
//    getCurrentSession()，要么用 subscribeSession() 拿事件。

/** 会话被替换时，这些监听器会整体重新绑到新会话上。 */
const listeners = new Set();

let runtime = null;
let starting = null;
let detachSession = null;

/** 把事件分发给所有监听器。单个监听器抛错不影响其它监听器。 */
function fanout(event) {
  for (const listener of listeners) {
    try {
      listener(event);
    } catch (error) {
      console.error("[Pi] 事件监听器抛出异常：", error);
    }
  }
}

/** 把订阅与扩展绑定挂到给定会话上。初始创建和每次替换后都要调用。 */
async function bindSession(session) {
  detachSession?.();
  detachSession = null;

  // 扩展绑定点，同时也是扩展收到 session_start 的地方。
  // 当前没有安装任何扩展，所以是空操作；但会话被替换后必须重新绑定，
  // 否则扩展拿不到新会话的启动事件。
  await session.bindExtensions({});

  detachSession = session.subscribe(fanout);
}

/**
 * 运行时工厂。创建新会话和切换会话时都会被调用。
 *
 * 必须按传入的 cwd 重建整套与工作目录相关的服务（设置、模型运行时、
 * 资源加载器），这是「切换会话即切换工作目录」能生效的原因 ——
 * 切到别的目录时会按那个目录重新发现 AGENTS.md、.pi/settings.json 等。
 *
 * 写法与 SDK 自带的 examples/sdk/13-session-runtime.ts 一致。
 *
 * 注意没有传 projectTrustContext：Pi 的项目信任在无法交互询问时
 * （非 TUI 模式下 defaultProjectTrust 的 "ask"）会跳过受保护的项目本地资源。
 * 这与引入本模块之前的实际行为相同，保持不动。
 */
async function createRuntime({ cwd, agentDir, sessionManager, sessionStartEvent }) {
  const services = await createAgentSessionServices({ cwd, agentDir });

  const created = await createAgentSessionFromServices({
    services,
    sessionManager,
    sessionStartEvent,
  });

  return {
    ...created,
    services,
    diagnostics: services.diagnostics,
  };
}

function reportDiagnostics(result) {
  for (const diagnostic of result.diagnostics ?? []) {
    const level = diagnostic.type === "error" ? "error" : "warn";
    console[level](`[Pi] ${diagnostic.type}：${diagnostic.message}`);
  }

  if (result.modelFallbackMessage) {
    console.warn("[Pi] 模型回退：", result.modelFallbackMessage);
  }
}

/** 用给定的会话管理器把运行时建起来，并挂上扩展绑定与事件订阅。 */
async function initializeRuntime({ cwd, sessionManager }) {
  const created = await createAgentSessionRuntime(createRuntime, {
    cwd,
    agentDir: getAgentDir(),
    sessionManager,
  });

  if (typeof created.session?.prompt !== "function") {
    throw new Error("Pi SDK 返回的会话没有 prompt() 方法");
  }

  reportDiagnostics(created);

  created.setRebindSession(bindSession);
  await bindSession(created.session);

  runtime = created;

  console.log("[Pi] 运行时初始化成功");
  console.log("[Pi] 工作目录：", created.cwd);
  console.log("[Pi] 会话文件：", created.session.sessionFile ?? "（未落盘）");

  return created;
}

/**
 * 懒创建运行时。并发调用共享同一次创建。
 *
 * @param {string} cwd 初始工作目录，仅在首次创建时生效。
 */
export async function getRuntime(cwd) {
  if (runtime) {
    return runtime;
  }

  if (starting) {
    return starting;
  }

  starting = (async () => {
    console.log("[Pi] 正在初始化运行时...");

    // SessionManager.create() 是懒写的：第一条消息落盘前不会写出文件。
    const sessionManager = SessionManager.create(cwd);

    return await initializeRuntime({
      cwd: sessionManager.getCwd(),
      sessionManager,
    });
  })();

  try {
    return await starting;
  } finally {
    starting = null;
  }
}

/** 当前会话。未初始化时返回 null。不要缓存这个返回值。 */
export function getCurrentSession() {
  return runtime?.session ?? null;
}

/** 当前生效的工作目录。未初始化时返回 null。 */
export function getCurrentCwd() {
  return runtime?.cwd ?? null;
}

export function isInitialized() {
  return runtime !== null;
}

export function isStreaming() {
  return runtime?.session?.isStreaming ?? false;
}

/**
 * 订阅当前会话的事件流。
 * 返回取消订阅函数，语义与 session.subscribe() 一致。
 *
 * 与直接订阅 session 的区别：会话被替换后，这个订阅会自动接到新会话上。
 */
export function subscribeSession(listener) {
  listeners.add(listener);

  return () => {
    listeners.delete(listener);
  };
}

/**
 * 切换到指定会话文件。
 *
 * cwd 会跟着会话记录里的工作目录走，这是 Pi 的原生行为
 * （AgentSessionRuntime.switchSession 用会话 header 里的 cwd 重建运行时）。
 *
 * 运行时还没建起来也能直接切：服务刚开、用户一次都没聊过就在树里点了历史会话，
 * 是正常路径。这时不先建一个临时会话再切（那样会按两个不同的 cwd 各建一次服务），
 * 而是直接把运行时建在目标会话上 —— 与 switchSession 内部做的事一致。
 *
 * 目标工作目录不存在时抛 MissingSessionCwdError。SDK 主入口没有导出这个类，
 * 只能按 error.name === "MissingSessionCwdError" 判断。
 */
export async function switchToSession(sessionPath, options) {
  if (!runtime) {
    console.log("[Pi] 运行时尚未初始化，直接建在目标会话上");

    // 与 AgentSessionRuntime.switchSession 内部一致：不传 sessionDir，
    // 由文件所在目录推导；cwd 取会话 header 里的工作目录。
    const sessionManager = SessionManager.open(sessionPath, undefined, options?.cwdOverride);

    await initializeRuntime({
      cwd: sessionManager.getCwd(),
      sessionManager,
    });

    return { cancelled: false };
  }

  const result = await runtime.switchSession(sessionPath, options);

  if (!result.cancelled) {
    console.log("[Pi] 已切换会话：", runtime.session.sessionFile ?? "（未落盘）");
    console.log("[Pi] 工作目录：", runtime.cwd);
  }

  return result;
}

/** 关闭运行时并清空订阅。用于进程退出。 */
export async function disposeRuntime() {
  detachSession?.();
  detachSession = null;
  listeners.clear();

  const current = runtime;
  runtime = null;

  if (current) {
    await current.dispose();
  }
}

/** 当前会话正在用的文件路径。未初始化、或新会话还没落盘时返回 null。 */
export function getCurrentSessionFile() {
  return runtime?.session?.sessionFile ?? null;
}

/**
 * 当前会话的对话消息，转成前端直接渲染的形状。
 *
 * 取的是 sessionManager.getBranch()（从根走到叶的原始条目）而不是
 * session.messages：
 *
 *   - getBranch() 只走当前分支，分叉过的会话不会把另一条分支的内容也读出来
 *   - 每个条目带着稳定的 id，正好给前端当列表 key
 *   - session.messages 是送去模型那份上下文，system 提示词、压缩摘要、
 *     bash 执行记录都在里面，不是聊天区该显示的东西
 *
 * 压缩（compaction）在这里不做特殊处理：被压缩掉的原始消息条目仍然留在分支上，
 * 所以读出来的是完整历史。对「会话历史」这个功能来说，完整比精简更合适。
 *
 * 运行时未初始化时返回空数组 —— 这个函数也会被只读接口用到
 * （看列表不该顺手把运行时建起来）。
 */
export function getCurrentMessages() {
  const session = runtime?.session;

  if (!session) {
    return [];
  }

  return toChatMessages(session.sessionManager.getBranch());
}

/**
 * 给会话设置显示名。
 *
 * 重命名不是重写文件：SDK 的做法是往文件末尾追加一条 session_info 条目，
 * 会话文件是 append-only 的（appendSessionInfo 会顺手把换行清成空格并 trim）。
 * 传空串就是清除名字，标题会退回第一条用户消息。
 *
 * 当前会话用运行时手里那个 SessionManager 而不是另开一个：它的 leaf 指针要跟着
 * 这条 session_info 走，否则文件里会多出一条和下一条消息并列的分支
 * （不影响读，但白白把树弄乱）。新会话还没落盘时也只更新内存 —— 这正是想要的，
 * 等它第一次落盘时这条名字会一起被写出去。
 *
 * @param {string} sessionPath 目标会话文件（调用方已经过 9.2 的范围断言）
 * @param {string} name 新名字，空串表示清除
 * @returns {string} 追加的条目 id
 */
export function renameSessionFile(sessionPath, name) {
  const current = runtime?.session;

  if (current?.sessionFile && samePath(current.sessionFile, sessionPath)) {
    return current.sessionManager.appendSessionInfo(name);
  }

  const sessionManager = SessionManager.open(sessionPath);

  return sessionManager.appendSessionInfo(name);
}
