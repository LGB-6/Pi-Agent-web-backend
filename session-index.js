/**
 * 会话索引
 *
 * 把 SessionManager.listAll() 的平铺结果整理成前端要的两级树：
 *   第一级 = 工作目录，第二级 = 该目录下的会话。
 *
 * 顺带维护一份 id → 文件路径 的缓存。理由见设计 9.1：
 * SessionManager.findById() 需要 cwd，而 PATCH / DELETE 的调用方只知道 id。
 *
 * 设计文档：docs/session-history-design.md 第八、九节
 */

import { existsSync } from "node:fs";
import path from "node:path";

import { getAgentDir, SessionManager } from "@earendil-works/pi-coding-agent";

/**
 * id → { path, cwd, cwdStatus }。每次列举时整体刷新。
 *
 * 刷新而不是累加：会话可能被外部（终端里的 pi、其他工具）删掉，
 * 累加式的缓存会留下指向已删除文件的条目。
 */
const entriesById = new Map();

/** 用 id 取路径。未命中返回 null，调用方需要自跑一次列举兜底。 */
export function getSessionPathById(id) {
  return entriesById.get(id)?.path ?? null;
}

/** 缓存条目数，供自检与日志使用。 */
export function indexedSessionCount() {
  return entriesById.size;
}

/**
 * 把某个 id 从缓存里摘掉。
 *
 * 删除成功后必须调用：否则在下一次列举之前，resolveSession() 还会把这个 id
 * 指回已经不存在的文件，接着切过去就会去 open 一个不存在的路径 ——
 * 而 SessionManager.open() 遇到不存在的文件会**悄悄新建一个空会话**。
 */
export function forgetSession(id) {
  entriesById.delete(id);
}

/**
 * Windows 上盘符大小写不固定，路径比对统一按平台规则归一。
 *
 * 导出是给「路径是否落在某个根之内」这类断言用的（文件树的范围检查）。
 * 这类断言必须跟这里用同一套归一规则 —— 各写一份迟早会分叉，
 * 而分叉的方向通常是"某一处漏判"，也就是越界。
 */
export function normalizeForCompare(target) {
  const resolved = path.resolve(target);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

/** 两个路径是不是同一个文件。供 IS_ACTIVE 判断与上层复用。 */
export function samePath(a, b) {
  if (!a || !b) {
    return false;
  }
  return normalizeForCompare(a) === normalizeForCompare(b);
}

/** 会话文件的根目录。与 SDK 内部的 getSessionsDir() 同一算法（那个函数没从主入口导出）。 */
export function sessionScopeRoot() {
  return path.join(getAgentDir(), "sessions");
}

/** 目标路径不在会话目录范围内（设计 9.2 的断言失败）。 */
export class OutOfScopeError extends Error {
  constructor(message) {
    super(message);
    this.name = "OutOfScopeError";
  }
}

/**
 * 断言目标路径确实位于会话目录之内，并且是 .jsonl。
 *
 * 设计 9.2：**动文件之前**必须过这一道。否则 DELETE 就是一个任意文件删除漏洞，
 * 配合启动设计里那个开放跨域，后果是灾难性的。
 *
 * 调用方传的从来只有 id，路径一律来自 SessionManager 的列举结果，所以正常情况
 * 下这个断言不会失败 —— 但"路径来自 SDK"是个应该被显式断言、而不是默认成立的性质：
 * 会话目录可以被项目设置改到别处，那次列举也许来自另一个根。
 *
 * @returns {string} 归一后的绝对路径
 * @throws {OutOfScopeError}
 */
export function assertSessionPathInScope(target) {
  if (typeof target !== "string" || !target) {
    throw new OutOfScopeError("会话路径为空");
  }

  const resolved = path.resolve(target);
  const root = path.resolve(sessionScopeRoot());

  // 带分隔符比对，否则 <root>-evil 这种同级目录会被前缀匹配放过去
  const prefix = normalizeForCompare(root) + path.sep;

  if (!normalizeForCompare(resolved).startsWith(prefix)) {
    throw new OutOfScopeError(`路径不在会话目录内：${resolved}`);
  }

  if (!resolved.toLowerCase().endsWith(".jsonl")) {
    throw new OutOfScopeError(`不是会话文件：${resolved}`);
  }

  return resolved;
}

/**
 * 工作目录的三种状态，不是布尔值。
 *
 * - `unknown`：旧版本会话，header 里根本没有 cwd。
 *   注意这一类**不会**在切换时报错，会静默落到服务进程的当前目录（设计 7.5），
 *   所以前端必须和 `missing` 一样置灰、一样不可点。
 */
function cwdStatusOf(cwd) {
  if (!cwd) {
    return "unknown";
  }
  return existsSync(cwd) ? "ok" : "missing";
}

/**
 * SessionInfo → 前端用的摘要。
 *
 * 刻意丢掉 allMessagesText：那是整段对话正文的拼接，会话搜索已经砍掉了，
 * 没有理由把它序列化到响应里。
 */
function toSummary(info) {
  return {
    id: info.id,
    path: info.path,
    name: info.name,
    firstMessage: info.firstMessage,
    messageCount: info.messageCount,
    created: info.created,
    modified: info.modified,
    isForked: Boolean(info.parentSessionPath),
  };
}

function latestActivity(group) {
  return group.sessions[0]?.modified?.getTime() ?? 0;
}

/**
 * 列举会话并分组。
 *
 * @param {string} currentCwd 当前工作目录，对应的分组会被置顶。
 * @param {string} [sessionDir] 会话目录，默认与 Pi 一致（~/.pi/agent/sessions）。
 * @returns {Promise<Array<{cwd: string, cwdStatus: string, sessions: object[]}>>}
 */
export async function listSessionGroups(currentCwd, sessionDir) {
  const infos = sessionDir
    ? await SessionManager.listAll(sessionDir)
    : await SessionManager.listAll();

  entriesById.clear();

  const byCwd = new Map();

  for (const info of infos) {
    // 空 cwd 用空串作分组键，多个旧会话会归到同一组
    const key = info.cwd || "";

    let group = byCwd.get(key);
    if (!group) {
      group = { cwd: key, cwdStatus: cwdStatusOf(key), sessions: [] };
      byCwd.set(key, group);
    }

    entriesById.set(info.id, {
      path: info.path,
      cwd: group.cwd,
      cwdStatus: group.cwdStatus,
    });

    group.sessions.push(toSummary(info));
  }

  const groups = [...byCwd.values()];

  // 组内按最近修改倒序
  for (const group of groups) {
    group.sessions.sort((a, b) => b.modified - a.modified);
  }

  // 分组按最近活跃倒序
  groups.sort((a, b) => latestActivity(b) - latestActivity(a));

  // 当前工作目录置顶。
  // 不依赖"最近活跃"自然置顶：刚重启、当前会话还没落盘时，这一组可能不在最前。
  const currentIndex = currentCwd
    ? groups.findIndex((group) => samePath(group.cwd, currentCwd))
    : -1;

  if (currentIndex > 0) {
    const [current] = groups.splice(currentIndex, 1);
    groups.unshift(current);
  }

  return groups;
}

/**
 * 把 id 解析成 { path, cwd, cwdStatus }，解析不到返回 null。
 *
 * 设计 9.1：缓存优先，未命中（服务重启过，或会话是终端里新建的）就重新列举一次
 * 兜底 —— 列举同时会把缓存刷新。
 *
 * 只认 id、不接受调用方传路径：路径一律来自 SessionManager 的列举结果，
 * 这样文件系统访问范围天然被限制在会话目录内（设计 9.2 的前提）。
 */
export async function resolveSession(id, sessionDir) {
  if (!id) {
    return null;
  }

  const cached = entriesById.get(id);
  if (cached) {
    return cached;
  }

  await listSessionGroups(null, sessionDir);

  return entriesById.get(id) ?? null;
}
