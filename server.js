
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { existsSync } from "node:fs";
import { readdir, realpath, stat, unlink } from "node:fs/promises";
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
  resetToWorkspace,
  subscribeSession,
  switchToSession,
} from "./pi-runtime.js";
import {
  assertSessionPathInScope,
  forgetSession,
  listSessionGroups,
  normalizeForCompare,
  resolveSession,
  samePath,
} from "./session-index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

app.use(cors());
app.use(express.json({ limit: "2mb" }));

// ============================================================
// 启动工作区
// ============================================================
//
// 由 startServer() 注入 —— 这个模块不再自己猜目录。
//
// 「你在哪个目录敲的命令」只有入口知道，模块里推断不出来：
//   - npm start 的 cwd 是 backend/，不是仓库根
//   - 全局安装后 __dirname/.. 会变成 pi-agent-web 自己的安装目录，
//     工作区成了自己的源码，模型会在里面改文件（见 startup-design 4.1）
//
// 将来由 bin/pi-web.js 传 process.cwd() 进来；现在由文末的开发入口传。

let launchCwd = null;

/**
 * 当前生效的工作目录。
 *
 * 运行时的会话自带 cwd（Pi 原生：换会话即换工作目录），所以有运行时听它的；
 * 还没有运行时（刚启动、一次都没聊过）才退回启动工作区。
 */
function effectiveCwd() {
  return getCurrentCwd() ?? launchCwd;
}

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
  const currentCwd = effectiveCwd();

  res.json({
    success: true,
    message: "Pi Agent Web 后端运行正常",
    // 当前生效的工作目录。有运行时就是会话自己的 cwd —— 它可能已经不是启动时
    // 那个目录了（切到另一个目录下的历史会话就会跟着变，这是 Pi 的原生行为）。
    workspace: currentCwd,
    // 启动工作区，进程级、一辈子不变。前端拿它跟上面那个比：两者不同就说明
    // 「已经离开启动目录」，该给一条回来的路。
    // 判断放后端做：路径归一（Windows 盘符大小写）已经有 samePath 了，
    // 没必要让前端再实现一遍。
    launchWorkspace: launchCwd,
    isLaunchWorkspace: samePath(currentCwd, launchCwd),
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
    const currentCwd = effectiveCwd();
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
      currentCwd: effectiveCwd(),
    });
  }

  if (isSwitching) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "正在切换会话，请稍后再试。",
      currentCwd: effectiveCwd(),
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
        currentCwd: effectiveCwd(),
        error: "这个会话没有记录工作目录，无法切换。",
      });
    }

    if (target.cwdStatus === "missing") {
      return res.status(409).json({
        success: false,
        code: "CWD_MISSING",
        reason: "not-exists",
        sessionCwd: target.cwd,
        currentCwd: effectiveCwd(),
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
      currentCwd: effectiveCwd(),
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
        currentCwd: effectiveCwd(),
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
// 会话历史：回到启动工作区开新会话
// ============================================================

/**
 * POST /api/sessions/new —— 把运行时搬回启动工作区，并开一个空会话。
 *
 * 为什么需要它：卡片上的工作目录是「会话自己的 cwd」，切到另一个目录下的历史会话
 * 之后它就跟着跑了，而且**回不去** —— 启动工作区可能一条会话都不剩（把它的会话
 * 删光之后就是这样），树里没有节点可点。这个接口是那条回来的路。
 *
 * 约束与切换会话完全相同：生成期间不能重建运行时（disposeRuntime 会清掉事件订阅，
 * 正在流式输出的那次请求会静默断掉），所以两道锁都要挡在前面。
 */
app.post("/api/sessions/new", async (req, res) => {
  if (isGenerating()) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "当前会话正在生成，无法新建。",
      currentCwd: effectiveCwd(),
    });
  }

  if (isSwitching) {
    return res.status(409).json({
      success: false,
      code: "BUSY",
      error: "正在切换会话，请稍后再试。",
      currentCwd: effectiveCwd(),
    });
  }

  isSwitching = true;

  try {
    await resetToWorkspace(launchCwd);

    console.log(`[会话] 已在启动工作区新建会话：${launchCwd}`);

    return res.json({
      success: true,
      currentCwd: effectiveCwd(),
      // 新会话是懒写的，此刻还没有文件 —— 前端按路径匹配自然匹配不到任何节点
      // （设计 7.2）。这一条与切换会话返回的形状刻意保持一致。
      activeSessionPath: getCurrentSession()?.sessionFile ?? null,
      // 空会话，聊天区要清空。
      messages: getCurrentMessages(),
    });
  } catch (error) {
    console.error("[会话] 新建会话失败：", error);

    return res.status(500).json({
      success: false,
      code: "NEW_SESSION_FAILED",
      error: error instanceof Error ? error.message : "新建会话失败",
    });
  } finally {
    isSwitching = false;
  }
});

// ============================================================
// 工作区文件树（只读）
// ============================================================
//
// 为什么不直接递归扫一遍工作目录：真正的项目底下必然有 node_modules。
// 递归扫描轻则几十万条目、卡死浏览器，重则把后端拖垮。所以这个接口是
// 「懒加载 + 单目录条数上限 + 默认隐藏」三者一起才成立的。

/**
 * 默认隐藏的目录名。
 *
 * 两个理由缺一不可：node_modules 这类目录动辄几万条目，不隐藏的话展开一次
 * 就够让面板卡住；而且它们对「看看这个项目长什么样」几乎没有信息量，纯噪音
 * —— VS Code 的 files.exclude 默认也是这个思路。
 *
 * 按名字匹配，不分文件与目录：叫 dist 的文件同样没必要占一行。
 * 被隐藏的条目会累计进响应的 hiddenCount，前端要拿它说明一句 ——
 * 否则用户看到的是一块缺了角的列表，会以为目录本来就是空的。
 *
 * 注意 .env 这类点文件**不在**清单里，名字会显示出来，但接口只列目录、
 * 不读文件内容，所以不构成泄露。
 */
const HIDDEN_NAMES = new Set([
  "node_modules",
  ".git",
  "dist",
  "build",
  "out",
  "coverage",
  ".venv",
  "venv",
  "__pycache__",
  ".cache",
  ".next",
  ".nuxt",
  "target",
  ".idea",
  ".vscode",
]);

/**
 * 三个上限挡的是三种不同形状的爆炸：
 *   - 单目录条数：一个目录里塞了几万个文件（打包产物、日志目录）
 *   - 展开目录数：depth=2 时根下有 200 个目录，每个都要读一遍
 *   - 总条目数：前两个都没单独触顶、但乘起来依然很大
 *
 * 触顶只停止**继续展开**，已经列出来的条目照常返回，也不报错 ——
 * 截断是正常情况，不是失败。
 */
const MAX_ENTRIES_PER_DIR = 500;
const MAX_EXPANDED_DIRS = 30;
const MAX_TOTAL_ENTRIES = 2000;

/** 目录名匹配用的大小写规则：Windows 文件系统不区分大小写，NODE_MODULES 就是 node_modules。 */
function isHiddenName(name) {
  return HIDDEN_NAMES.has(name.toLowerCase());
}

/**
 * 排序规则。模块级缓存一个 collator —— localeCompare 每次调用都会重新构造
 * 一个 Collator，一个 500 条的目录排下来是几千次构造，没必要。
 *
 * numeric 让 file2 排在 file10 前面，比纯字典序符合直觉。
 */
const nameCollator = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: "base",
});

/**
 * target 是否位于 base 之内（**含 base 本身**）。
 *
 * 比对规则与 session-index 的 assertSessionPathInScope 一致：归一后带
 * path.sep 比前缀，否则 <base>-evil 这种同级目录会被前缀匹配放过去。
 * 区别是这里接受 base 本身 —— 文件树要能列根目录。
 */
function isInside(target, base) {
  const normalizedTarget = normalizeForCompare(target);
  const normalizedBase = normalizeForCompare(base);

  return (
    normalizedTarget === normalizedBase ||
    normalizedTarget.startsWith(normalizedBase + path.sep)
  );
}

/** 绝对路径 → 相对工作目录的路径，分隔符统一成 /（前端只用这一种形式）。 */
function relativeFromBase(absTarget, base) {
  return path.relative(base, absTarget).split(path.sep).join("/");
}

/** 目录在前、文件在后（软链接算文件）；组内按名字，numeric 让 file2 排在 file10 前。 */
function compareDirents(a, b) {
  const aIsDir = a.isDirectory();
  const bIsDir = b.isDirectory();

  if (aIsDir !== bIsDir) {
    return aIsDir ? -1 : 1;
  }

  return nameCollator.compare(a.name, b.name);
}

/**
 * 读一个目录，返回它的直接子项；depth > 1 时再往下展开一层。
 *
 * @param {string} absDir 已确认在范围内、且已取过 realpath 的绝对路径
 * @param {string} relDir 相对工作目录的路径，"" 表示根
 * @param {number} depth 还要往下展开几层
 * @param {string} base 工作目录的真实路径，递归展开时要用它再验一次范围
 * @param {{expandedDirs:number,totalEntries:number,hidden:number,truncated:boolean}} budget
 */
async function readDirectory(absDir, relDir, depth, base, budget) {
  const dirents = await readdir(absDir, { withFileTypes: true });

  const visible = [];
  let hiddenHere = 0;

  for (const dirent of dirents) {
    if (isHiddenName(dirent.name)) {
      hiddenHere++;
    } else {
      visible.push(dirent);
    }
  }

  budget.hidden += hiddenHere;
  visible.sort(compareDirents);

  let truncated = false;

  if (visible.length > MAX_ENTRIES_PER_DIR) {
    visible.length = MAX_ENTRIES_PER_DIR;
    truncated = true;
  }

  const entries = [];
  const subdirs = [];

  for (const dirent of visible) {
    if (budget.totalEntries >= MAX_TOTAL_ENTRIES) {
      truncated = true;
      break;
    }

    budget.totalEntries++;

    const relPath = relDir ? `${relDir}/${dirent.name}` : dirent.name;

    // 软链接一律不跟随。
    //
    // 一个指向工作区外的软链接就是一条越界通道，而「跟着它走」要额外做一次
    // realpath + 范围断言才安全。用「不跟随」一次性消掉整类风险 —— 代价是
    // 工作区内合法的目录软链也不能展开，会显示成一个叶子节点。只读浏览
    // 可以接受；将来真需要支持，再单独给这一条加上那两次检查。
    if (dirent.isSymbolicLink()) {
      entries.push({ name: dirent.name, path: relPath, type: "symlink" });
      continue;
    }

    if (dirent.isDirectory()) {
      const entry = { name: dirent.name, path: relPath, type: "directory" };

      entries.push(entry);
      subdirs.push({ entry, absDir: path.join(absDir, dirent.name) });
      continue;
    }

    entries.push({ name: dirent.name, path: relPath, type: "file" });
  }

  budget.truncated = budget.truncated || truncated;

  // 本层全部列完再往下展开 —— 否则总量触顶时会出现「前半截展开过、后半截空着」
  // 的参差，看起来像是那些目录本来就是空的。
  if (depth > 1) {
    for (const { entry, absDir: childAbsDir } of subdirs) {
      if (budget.expandedDirs >= MAX_EXPANDED_DIRS) break;
      if (budget.totalEntries >= MAX_TOTAL_ENTRIES) break;

      budget.expandedDirs++;

      try {
        // 展开之前再验一次范围。
        //
        // 上面那道 isSymbolicLink() 已经拦掉了软链接和 junction（实测 Node 把
        // junction 也报成 symbolic link），所以这一步在当前平台上是冗余的。
        // 留着是因为它不是同一类检查：那一道看的是「这个条目长什么样」，
        // 这一道看的是「这条路径实际通向哪里」。将来 Node 对某种 reparse point
        // （比如把整个卷挂到目录上的 mount point）改判为目录，或者哪个平台
        // 的 readdir 不上报链接类型，只有这一道还兜得住 —— 而漏掉它的后果是
        // 工作区外的目录内容直接列进树里。一次 realpath 换这个，很划算。
        //
        // 展开用 realChild 而不是 childAbsDir：既然已经取到真实路径，就用它。
        const realChild = await realpath(childAbsDir);

        if (!isInside(realChild, base)) {
          continue;
        }

        entry.children = await readDirectory(
          realChild,
          entry.path,
          depth - 1,
          base,
          budget,
        );
      } catch {
        // 子目录读不了（权限、列举途中被删）不该让整份响应失败：
        // 保留这个目录节点，只是不展开它。
      }
    }
  }

  return entries;
}

app.get("/api/files", async (req, res) => {
  // ?path=a&path=b 在 Express 里会解析成数组。遇上这种就直接拒 ——
  // 静默当成空串的话，一个畸形请求会拿到工作区根目录的列表，看着像成功。
  if (req.query.path !== undefined && typeof req.query.path !== "string") {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "path 参数不合法。",
    });
  }

  const rawPath = req.query.path ?? "";

  if (rawPath.includes("\0")) {
    return res.status(400).json({
      success: false,
      code: "INVALID_REQUEST",
      error: "路径不合法。",
    });
  }

  const parsedDepth = Number(req.query.depth ?? 1);
  const depth = Number.isFinite(parsedDepth)
    ? Math.min(Math.max(Math.trunc(parsedDepth), 1), 2)
    : 1;

  // 1. 定位工作目录。它本身可能就是软链接（macOS 上 /tmp 即是 /private/tmp），
  //    先取真实路径，后面所有比对都以它为准。
  let base;

  try {
    base = await realpath(effectiveCwd());
  } catch {
    return res.status(404).json({
      success: false,
      code: "CWD_MISSING",
      error: "当前工作目录不存在，无法列出文件。",
    });
  }

  // 2. 词法归一 + 范围断言。这一步挡住的是 "../../" 这类纯文本的越界：
  //    resolve 会把 .. 折叠掉，折完还在 base 外就直接拒。
  const target = path.resolve(base, rawPath);

  if (!isInside(target, base)) {
    return res.status(400).json({
      success: false,
      code: "OUT_OF_SCOPE",
      error: "只能浏览当前工作目录下的文件。",
    });
  }

  // 3. 取真实路径后再断言一次。**这一步不能省** —— 第 2 步只看得到词法路径，
  //    工作区里放一个指向 C:\Windows 的软链接就能整个绕过去。
  let real;

  try {
    real = await realpath(target);
  } catch {
    return res.status(404).json({
      success: false,
      code: "NOT_FOUND",
      error: "这个目录不存在。",
    });
  }

  if (!isInside(real, base)) {
    return res.status(400).json({
      success: false,
      code: "OUT_OF_SCOPE",
      error: "只能浏览当前工作目录下的文件。",
    });
  }

  // 4. 确认它确实是个目录。对文件请求返回 404 而不是让 readdir 去抛平台
  //    各异的错误码（Windows 和 Linux 在这里的行为不完全一样）。
  try {
    if (!(await stat(real)).isDirectory()) {
      return res.status(404).json({
        success: false,
        code: "NOT_FOUND",
        error: "这不是一个目录。",
      });
    }
  } catch {
    return res.status(404).json({
      success: false,
      code: "NOT_FOUND",
      error: "这个目录不存在。",
    });
  }

  try {
    const budget = {
      expandedDirs: 0,
      totalEntries: 0,
      hidden: 0,
      truncated: false,
    };

    const entries = await readDirectory(real, "", depth, base, budget);

    // cwd 与 path 都回显归一后的值：客户端可能传 "a/../b" 或大小写不同的
    // 盘符，前端要用这个 path 去对 childMap 的 key。
    return res.json({
      success: true,
      cwd: base,
      path: relativeFromBase(real, base),
      entries,
      truncated: budget.truncated,
      hiddenCount: budget.hidden,
    });
  } catch (error) {
    console.error("[文件] 列举失败：", error);

    return res.status(500).json({
      success: false,
      code: "LIST_FAILED",
      error: error instanceof Error ? error.message : "读取目录失败",
    });
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

    const runtime = await getRuntime(launchCwd);
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

let shuttingDown = false;

/**
 * 优雅退出：先关运行时让 Pi 侧收尾（扩展收到 session_shutdown、会话落盘），
 * 再关闭 HTTP 服务。
 */
async function shutdown(server, signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  console.log(`\n[服务] 收到 ${signal}，正在关闭...`);

  try {
    await disposeRuntime();
  } catch (error) {
    console.error("[Pi] 关闭运行时失败：", error);
  }

  server.close(() => {
    process.exit(0);
  });
}

/**
 * 启动服务。
 *
 * cwd 必须由入口显式给出，这个模块不做任何推断 —— 理由见上面「启动工作区」那段。
 * 将来 bin/pi-web.js 传 process.cwd()，开发时走文末的开发入口。
 *
 * @param {object} options
 * @param {string} options.cwd 启动工作区（相对路径按当前进程的 cwd 解析）
 * @param {number} [options.port]
 * @param {string} [options.host]
 * @returns {Promise<import("node:http").Server>} 已开始监听的 HTTP 服务
 */
export async function startServer({ cwd, port = 3000, host = "127.0.0.1" }) {
  if (!cwd) {
    throw new Error("startServer 需要 cwd：启动工作区必须由入口显式给出");
  }

  launchCwd = path.resolve(cwd);

  const server = await new Promise((resolve) => {
    const created = app.listen(port, host, () => {
      console.log("========================================");
      console.log(" Pi Agent Web 后端");
      console.log("========================================");
      console.log(`[服务] http://${host}:${port}`);
      console.log(`[健康检查] http://${host}:${port}/api/health`);
      console.log(`[启动工作区] ${launchCwd}`);
      resolve(created);
    });
  });

  // once 而不是 on：重复调用 startServer 不会把处理器叠起来。
  process.once("SIGINT", () => void shutdown(server, "SIGINT"));
  process.once("SIGTERM", () => void shutdown(server, "SIGTERM"));

  return server;
}

// ============================================================
// 开发入口
// ============================================================
//
// 只有这个文件被**直接执行**时才走这一段（npm start / npm run dev /
// 验证脚本里的 `node server.js`）。它负责算出「开发时的工作区」，再交给 startServer。
//
// .env 显式钉到 backend/.env：dotenv 默认读 process.cwd()/.env，换个目录启动就会
// 静默失效 —— 实测在仓库根跑 `node backend/server.js`，PI_WORKSPACE 读不到，
// 工作区悄悄退回仓库根。

if (samePath(process.argv[1], __filename)) {
  dotenv.config({ path: path.join(__dirname, ".env") });

  startServer({
    // 开发期仍然允许 PI_WORKSPACE 覆盖（验证脚本靠它做隔离），
    // 没设就退回仓库根。这条兜底只属于开发入口，不进模块。
    cwd: path.resolve(process.env.PI_WORKSPACE || path.resolve(__dirname, "..")),
    port: Number(process.env.PORT || 3000),
  }).catch((error) => {
    console.error("[服务] 启动失败：", error);
    process.exit(1);
  });
}
