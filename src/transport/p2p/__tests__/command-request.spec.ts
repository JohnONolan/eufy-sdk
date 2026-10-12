import { inspect } from "node:util";
import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DeviceCommandError } from "../../../core/contracts.js";
import { requestControlPayload } from "../command-request.js";
import type { P2PCommandResult, P2PFrame, P2PSession } from "../p2p-session.js";

const result = (code: number, overrides: Partial<P2PCommandResult> = {}): P2PCommandResult => ({
  code,
  channel: 0,
  commandId: 1350,
  dataType: 0,
  type: 1,
  ...overrides,
});
const session = (): P2PSession => new EventEmitter() as P2PSession;
const start = (peer: P2PSession, send = vi.fn(() => true), signal?: AbortSignal) =>
  requestControlPayload(peer, { command: 1940, channel: 0, timeoutMs: 10000, maxSends: 5, retryMs: 200, send, signal });
const clean = (peer: P2PSession): void => {
  for (const event of ["data", "commandResult", "close", "connect"]) expect(peer.listenerCount(event)).toBe(0);
  expect(vi.getTimerCount()).toBe(0);
};

describe("private-session command phase", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it.each([-106, -9876])("preserves rejection %s immediately and stops all retransmits", async (code) => {
    const peer = session();
    const send = vi.fn(() => true);
    const pending = start(peer, send);
    const rejected = expect(pending).rejects.toMatchObject({
      name: "DeviceCommandError",
      reason: "rejected",
      code,
      command: 1940,
      channel: 0,
    });
    peer.emit("commandResult", result(code));
    await rejected;
    await vi.advanceTimersByTimeAsync(10000);
    expect(send).toHaveBeenCalledTimes(1);
    clean(peer);
  });

  it("handles a result synchronously inside send without an unhandled rejection", async () => {
    const peer = session();
    const pending = start(
      peer,
      vi.fn(() => {
        peer.emit("commandResult", result(-106));
        return true;
      }),
    );
    await expect(pending).rejects.toBeInstanceOf(DeviceCommandError);
    clean(peer);
  });

  it("does not accept a result emitted by a send that was never submitted", async () => {
    const peer = session();
    const pending = start(
      peer,
      vi.fn(() => {
        peer.emit("commandResult", result(0));
        return false;
      }),
    );
    const rejected = expect(pending).rejects.toMatchObject({ reason: "send-failed" });
    await vi.advanceTimersByTimeAsync(800);
    await rejected;
    clean(peer);
  });

  it("retains a result from an earlier submission arriving during a failed retry", async () => {
    const peer = session();
    const send = vi
      .fn()
      .mockReturnValueOnce(true)
      .mockImplementationOnce(() => {
        peer.emit("commandResult", result(-106));
        return false;
      });
    const pending = start(peer, send);
    const assertion = expect(pending).rejects.toMatchObject({ reason: "rejected", code: -106 });
    await vi.advanceTimersByTimeAsync(200);
    await assertion;
    expect(send).toHaveBeenCalledTimes(2);
    clean(peer);
  });

  it("ignores other channels, wrappers, CONTROL, notifications and another session", async () => {
    const peer = session();
    const pending = start(peer);
    for (const override of [{ channel: 1 }, { commandId: 1700 }, { dataType: 2 }, { type: 0 }])
      peer.emit("commandResult", result(-106, override));
    session().emit("commandResult", result(-106));
    peer.emit("commandResult", result(0));
    await pending;
    clean(peer);
  });

  it("ACK-only delivery times out waiting for acceptance", async () => {
    const peer = session();
    const pending = start(peer);
    const rejected = expect(pending).rejects.toMatchObject({ reason: "acceptance-timeout", code: undefined });
    peer.emit("ack", 0);
    await vi.advanceTimersByTimeAsync(10000);
    await rejected;
    clean(peer);
  });

  it("acceptance stops replay but a query still needs its matched payload", async () => {
    const peer = session();
    const send = vi.fn(() => true);
    const pending = requestControlPayload(peer, {
      command: 1940,
      channel: 0,
      send,
      timeoutMs: 10000,
      maxSends: 5,
      retryMs: 200,
      readReply: (f) => (f.json?.matched === true ? "reply" : undefined),
    });
    peer.emit("commandResult", result(0));
    peer.emit("data", { channel: 1, json: { matched: true } });
    peer.emit("data", { channel: 0, json: { matched: false } });
    await vi.advanceTimersByTimeAsync(800);
    expect(send).toHaveBeenCalledTimes(1);
    expect(peer.listenerCount("data")).toBe(1);
    peer.emit("data", { channel: 0, json: { matched: true } });
    await expect(pending).resolves.toBe("reply");
    clean(peer);
  });

  it("distinguishes accepted but missing payload from missing acceptance", async () => {
    const peer = session();
    const pending = requestControlPayload(peer, {
      command: 1940,
      channel: 0,
      send: () => true,
      timeoutMs: 10000,
      maxSends: 5,
      retryMs: 200,
      readReply: () => undefined,
    });
    const rejected = expect(pending).rejects.toMatchObject({ reason: "response-timeout" });
    peer.emit("commandResult", result(0));
    await vi.advanceTimersByTimeAsync(10000);
    await rejected;
    clean(peer);
  });

  it("payload or local decode failure can settle a query before an acceptance result", async () => {
    const peer = session();
    const pending = requestControlPayload(peer, {
      command: 1940,
      channel: 0,
      send: () => true,
      timeoutMs: 10000,
      maxSends: 5,
      retryMs: 200,
      readReply: (_f: P2PFrame) => {
        throw new Error("PRIVATE_PAYLOAD_SENTINEL");
      },
    });
    const rejected = expect(pending).rejects.toMatchObject({ reason: "invalid-response" });
    peer.emit("data", { channel: 0 });
    await rejected;
    await pending.catch((error: Error) => {
      expect(String(error)).not.toContain("PRIVATE_PAYLOAD_SENTINEL");
      expect(JSON.stringify(error)).not.toContain("PRIVATE_PAYLOAD_SENTINEL");
      expect(inspect(error)).not.toContain("PRIVATE_PAYLOAD_SENTINEL");
      expect(error.cause).toBeUndefined();
    });
    clean(peer);
  });

  it.each(["close", "connect"])("refuses session %s and retires its resources", async (event) => {
    const peer = session();
    const pending = start(peer);
    const rejected = expect(pending).rejects.toMatchObject({ reason: "disconnected" });
    peer.emit(event);
    await rejected;
    clean(peer);
  });

  it("cleans up on caller cancellation", async () => {
    const peer = session();
    const controller = new AbortController();
    const pending = start(peer, undefined, controller.signal);
    const reason = new Error("cancelled");
    const rejected = expect(pending).rejects.toBe(reason);
    controller.abort(reason);
    await rejected;
    clean(peer);
  });

  it("late and duplicate results from a retired session cannot settle its successor", async () => {
    const first = session();
    const old = start(first);
    const rejected = expect(old).rejects.toMatchObject({ reason: "acceptance-timeout" });
    await vi.advanceTimersByTimeAsync(10000);
    await rejected;
    const second = session();
    const next = start(second);
    first.emit("commandResult", result(-106));
    first.emit("commandResult", result(0));
    expect(second.listenerCount("commandResult")).toBe(1);
    second.emit("commandResult", result(0));
    second.emit("commandResult", result(-106));
    await next;
    clean(first);
    clean(second);
  });
});
