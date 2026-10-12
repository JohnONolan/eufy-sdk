import { DeviceCommandError } from "../../core/contracts.js";
import type { P2PCommandResult, P2PFrame, P2PSession } from "./p2p-session.js";
import { P2P_ENVELOPE } from "./envelope.js";

const RESULT_DATA_TYPE = 0;
const RESULT_FRAME_TYPE = 1;

/**
 * One command phase on a fresh, private session. The caller must close the session in finally;
 * sharing or reusing it would make these wrapper-only results ambiguous. Incoming sequence numbers
 * are deliberately not used as outgoing request identifiers.
 */
export function requestControlPayload<T = void>(
  session: P2PSession,
  opts: {
    command: number;
    channel: number;
    send: () => boolean;
    timeoutMs: number;
    maxSends: number;
    retryMs: number;
    readReply?: (frame: P2PFrame) => T | undefined;
    signal?: AbortSignal;
  },
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let sent = false;
    let sending = false;
    let accepted = false;
    let sends = 0;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    const immediate: Array<() => void> = [];
    const context = { command: opts.command, channel: opts.channel };
    const cleanup = (): void => {
      clearTimeout(retry);
      clearTimeout(deadline);
      session.off("commandResult", onResult);
      session.off("data", onData);
      session.off("close", onClose);
      session.off("connect", onClose);
      opts.signal?.removeEventListener("abort", onAbort);
      immediate.length = 0;
    };
    const finish = (error?: unknown, value?: T): void => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error !== undefined) reject(error);
      else resolve(value as T);
    };
    const fail = (reason: ConstructorParameters<typeof DeviceCommandError>[0], code?: number): void =>
      finish(new DeviceCommandError(reason, context, code));
    const deliver = (work: () => void): void => {
      if (settled) return;
      if (sending) immediate.push(work);
      else if (sent) work();
    };
    const onResult = (result: P2PCommandResult): void => {
      if (
        result.dataType !== RESULT_DATA_TYPE ||
        result.type !== RESULT_FRAME_TYPE ||
        result.commandId !== P2P_ENVELOPE.SET_PAYLOAD ||
        result.channel !== opts.channel
      )
        return;
      deliver(() => {
        if (result.code !== 0) return fail("rejected", result.code);
        accepted = true;
        clearTimeout(retry);
        if (!opts.readReply) finish();
      });
    };
    const onData = (frame: P2PFrame): void => {
      if (!opts.readReply || frame.channel !== opts.channel) return;
      deliver(() => {
        try {
          const reply = opts.readReply!(frame);
          if (reply !== undefined) finish(undefined, reply);
        } catch {
          finish(new DeviceCommandError("invalid-response", context));
        }
      });
    };
    const onClose = (): void => fail("disconnected");
    const onAbort = (): void => finish(opts.signal!.reason);
    const transmit = (): void => {
      if (settled || accepted) return;
      sending = true;
      let submitted: boolean;
      try {
        submitted = opts.send();
      } catch {
        finish(new DeviceCommandError("send-failed", context));
        return;
      } finally {
        sending = false;
      }
      sends++;
      if (submitted) sent = true;
      if (sent) {
        for (const work of immediate.splice(0)) {
          if (settled) break;
          work();
        }
      } else {
        immediate.length = 0;
      }
      if (settled || accepted) return;
      if (sends < opts.maxSends) retry = setTimeout(transmit, opts.retryMs);
      else if (!sent) fail("send-failed");
    };
    if (opts.signal?.aborted) return finish(opts.signal.reason);
    session.on("commandResult", onResult);
    session.on("data", onData);
    session.on("close", onClose);
    session.on("connect", onClose);
    opts.signal?.addEventListener("abort", onAbort, { once: true });
    deadline = setTimeout(() => fail(accepted ? "response-timeout" : "acceptance-timeout"), opts.timeoutMs);
    transmit();
  });
}
