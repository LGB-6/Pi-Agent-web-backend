/**
 * 会话条目 → 聊天区消息
 *
 * 会话文件里存的 message 条目是给模型看的（content 是内容块数组，
 * 里面混着 thinking / toolCall / toolResult），而聊天区只渲染纯文本一行。
 * 这个模块负责那一步收窄，纯函数，不碰文件也不碰运行时，方便单独验。
 *
 * 设计文档：docs/session-history-design.md 第十节
 */

/**
 * 把一条消息的 content 压成纯文本。
 *
 * 数组形态里只取 text 块：thinking 是模型的思考过程、toolCall / toolResult
 * 是工具往返，都不是对话内容。多个 text 块用换行接起来（一次回复被工具调用
 * 打断成两段时会出现）。
 *
 * 字符串形态是防御性的 —— 当前 SDK 写出来的 user 消息也是数组，
 * 但 toolResult 之类历史版本里可能是裸字符串。
 */
function textOf(content) {
  if (typeof content === "string") {
    return content.trim();
  }

  if (!Array.isArray(content)) {
    return "";
  }

  return content
    .filter((block) => block?.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n")
    .trim();
}

/**
 * 取出可以显示的消息。
 *
 * 三类条目被跳过：
 *   - 非 message 条目（session_info / model_change / compaction / label …）
 *   - role 不是 user / assistant 的（system 是系统提示词，工具结果是 toolResult）
 *   - 压完之后没有文本的（被中断的空回复、只有工具调用的那一轮）
 *
 * 最后一条要留意：跳过它意味着"问了但没答"会显示成一条孤零零的提问，
 * 这是准确的 —— 硬塞一条空气泡反而像是应用坏了。
 *
 * @param {Array<{id?: string, type?: string, message?: {role?: string, content?: unknown}}>} entries
 *   一条分支上的会话条目，顺序即对话顺序（sessionManager.getBranch() 的返回值）
 * @returns {Array<{id: string, role: "user" | "assistant", content: string}>}
 */
export function toChatMessages(entries) {
  const messages = [];

  for (const entry of entries ?? []) {
    if (entry?.type !== "message") {
      continue;
    }

    const message = entry.message;

    if (!message || (message.role !== "user" && message.role !== "assistant")) {
      continue;
    }

    const content = textOf(message.content);

    if (!content) {
      continue;
    }

    // 用条目 id 当 key：消息自己不带 id，而条目 id 是稳定的，
    // 前端列表重排时不会把组件认错。
    messages.push({ id: entry.id, role: message.role, content });
  }

  return messages;
}
