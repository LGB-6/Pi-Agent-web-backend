
import "dotenv/config";
import path from "node:path";
import {
  createAgentSession,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

const cwd = path.resolve(process.cwd(), "..");

async function main() {
  let session;

  try {
    console.log("[1] 开始创建 Session");
    console.log("[环境] cwd =", cwd);
    console.log("[环境] HOME =", process.env.HOME);
    console.log("[环境] USERPROFILE =", process.env.USERPROFILE);

    const result = await createAgentSession({
      cwd,
      sessionManager: SessionManager.create(cwd),
    });

    session = result.session;

    console.log("[2] Session 创建完成");
    console.log("[模型]", session.model);
    console.log("[会话文件]", session.sessionFile);
    console.log("[模型回退信息]", result.modelFallbackMessage ?? "无");

    const unsubscribe = session.subscribe((event) => {
      const item = event.assistantMessageEvent;

      console.log(
        "[SDK 事件]",
        event.type,
        item?.type ?? "",
        item?.type === "text_delta" ? item.delta : ""
      );
    });

    console.log("[3] 开始调用 session.prompt()");
    console.log("[提示词] 请只回复：Hello");

    const timeout = setTimeout(() => {
      console.error("[超时] 30 秒后仍未完成 prompt()");
      console.error("[诊断] isStreaming =", session.isStreaming);
      console.error("[诊断] 当前模型 =", session.model);
    }, 30_000);

    try {
      await session.prompt("请只回复：Hello");
      console.log("[4] prompt() 已正常完成");
      console.log("[最终回复]", session.getLastAssistantText());
    } finally {
      clearTimeout(timeout);
      unsubscribe();
    }
  } catch (error) {
    console.error("[SDK 错误]", error);
    console.error(error?.stack ?? "");
  } finally {
    if (session) {
      session.dispose();
    }
  }
}

main().catch(console.error);
