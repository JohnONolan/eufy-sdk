import { getEventListeners } from "node:events";
import { buildFf09ResponseFrame } from "../../__tests__/ff09-test-fixtures.js";
import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { P2PCommandRouter } from "../command-router.js";
import { connectedSession, routerWithSession, DEVICE_SN, ACCOUNT_ID, STATION_SN } from "./session-fixtures.js";
import type { P2PSession } from "../p2p-session.js";

function peer() {
  return Object.assign(connectedSession(), {
    connect: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
    sendControlLevel2: vi.fn(
      (_command: number, _channel: number, _account: string, _payload: unknown, _value?: number) => true,
    ),
  });
}
const result = (code: number) => ({ code, channel: 1, commandId: 1350, dataType: 0, type: 1 });
const identity = {
  adminUserId: ACCOUNT_ID,
  deviceSn: DEVICE_SN,
  username: "synthetic",
  shortUserId: "0000",
  engage: true,
};
const actuate = (router: P2PCommandRouter) => router.dispatchCommand(DEVICE_SN, { kind: "ff09-actuate", ...identity });
function harness(sessions: ReturnType<typeof peer>[]) {
  const main = peer();
  const router = routerWithSession(main);
  const create = vi.spyOn(router as any, "createSession");
  for (const session of sessions) create.mockResolvedValueOnce(session);
  return { router, main, create };
}

describe("exclusive command connection ownership", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("propagates the read rejection without waiting for its payload or replaying", async () => {
    const session = peer();
    const { router, main } = harness([session]);
    const pending = router.getAutoLockState(DEVICE_SN, identity);
    const assertion = expect(pending).rejects.toMatchObject({
      reason: "rejected",
      code: -106,
      command: 1940,
      channel: 1,
    });
    await vi.advanceTimersByTimeAsync(0);
    main.emit("commandResult", result(-900));
    session.emit("commandResult", result(-106));
    await assertion;
    expect(session.sendControlLevel2).toHaveBeenCalledTimes(1);
    expect(session.close).toHaveBeenCalledOnce();
    expect(main.close).not.toHaveBeenCalled();
    expect(session.listenerCount("commandResult")).toBe(0);
  });

  it("a write submits once and requires acceptance; rejection reaches the action promise", async () => {
    const first = peer();
    const second = peer();
    const { router } = harness([first, second]);
    const rejected = actuate(router);
    const assertion = expect(rejected).rejects.toMatchObject({ reason: "rejected", code: -9876 });
    await vi.advanceTimersByTimeAsync(0);
    first.emit("commandResult", result(-9876));
    await assertion;
    const accepted = actuate(router);
    await vi.advanceTimersByTimeAsync(0);
    second.emit("commandResult", result(0));
    await accepted;
    expect(first.sendControlLevel2).toHaveBeenCalledOnce();
    expect(second.sendControlLevel2).toHaveBeenCalledOnce();
    expect(first.close).toHaveBeenCalledOnce();
    expect(second.close).toHaveBeenCalledOnce();
  });

  it("serializes same-channel phases and isolates late replies after timeout", async () => {
    const first = peer();
    const second = peer();
    const { router, create } = harness([first, second]);
    const old = actuate(router);
    const assertion = expect(old).rejects.toMatchObject({ reason: "acceptance-timeout" });
    const next = actuate(router);
    await vi.advanceTimersByTimeAsync(9999);
    expect(create).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    await assertion;
    expect(create).toHaveBeenCalledTimes(2);
    expect(first.close).toHaveBeenCalledOnce();
    first.emit("commandResult", result(0));
    first.emit("commandResult", result(-106));
    expect(second.listenerCount("commandResult")).toBe(1);
    second.emit("commandResult", result(0));
    await next;
    expect(second.close).toHaveBeenCalledOnce();
  });

  it("closes failed setup and releases the station reservation", async () => {
    const first = peer();
    first.connect.mockRejectedValueOnce(new Error("setup failed"));
    const second = peer();
    const { router } = harness([first, second]);
    const old = actuate(router);
    const assertion = expect(old).rejects.toThrow("setup failed");
    const next = actuate(router);
    await vi.advanceTimersByTimeAsync(0);
    await assertion;
    expect(first.close).toHaveBeenCalledOnce();
    second.emit("commandResult", result(0));
    await next;
  });

  it.each(["connect", "key"])("peer closure during %s setup rejects promptly and releases ownership", async (stage) => {
    const first = peer();
    if (stage === "connect") first.isConnected = false;
    else first.awaitLevel2Key.mockImplementation(() => new Promise(() => {}));
    const second = peer();
    const { router } = harness([first, second]);
    const old = actuate(router);
    const assertion = expect(old).rejects.toMatchObject({ reason: "disconnected" });
    const next = actuate(router);
    await vi.advanceTimersByTimeAsync(0);
    first.emit("close");
    await vi.advanceTimersByTimeAsync(0);
    await assertion;
    expect(first.sendControlLevel2).not.toHaveBeenCalled();
    expect(first.listenerCount("close")).toBe(0);
    second.emit("commandResult", result(0));
    await next;
  });

  it("router close invalidates active and queued phases without dialing a successor", async () => {
    const first = peer();
    const second = peer();
    const { router, create } = harness([first, second]);
    const active = actuate(router);
    const queued = actuate(router);
    const assertions = [active, queued].map((p) =>
      expect(p).rejects.toMatchObject({ reason: "disconnected", channel: 1 }),
    );
    await vi.advanceTimersByTimeAsync(0);
    await router.closeAll();
    await Promise.all(assertions);
    expect(create).toHaveBeenCalledOnce();
    expect(first.listenerCount("commandResult")).toBe(0);
    expect(second.connect).not.toHaveBeenCalled();
  });

  it("closes a session constructed after router shutdown without connecting it", async () => {
    const session = peer();
    const { router, create } = harness([]);
    let constructed!: (session: ReturnType<typeof peer>) => void;
    create.mockReturnValue(
      new Promise((resolve) => {
        constructed = resolve;
      }),
    );
    const pending = actuate(router);
    const assertion = expect(pending).rejects.toMatchObject({ reason: "disconnected" });
    await vi.advanceTimersByTimeAsync(0);
    await router.closeAll();
    constructed(session);
    await assertion;
    expect(session.connect).not.toHaveBeenCalled();
    expect(session.close).toHaveBeenCalledOnce();
  });

  it("refuses a reused settings timestamp before sending a successor query", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const first = peer();
    const second = peer();
    const { router } = harness([first, second]);
    const old = router.getAutoLockState(DEVICE_SN, identity);
    const assertion = expect(old).rejects.toMatchObject({ reason: "rejected" });
    await vi.advanceTimersByTimeAsync(0);
    first.emit("commandResult", result(-106));
    await assertion;
    const next = router.getAutoLockState(DEVICE_SN, identity);
    const refused = expect(next).rejects.toMatchObject({ reason: "correlation-unavailable" });
    await vi.advanceTimersByTimeAsync(0);
    await refused;
    expect(second.sendControlLevel2).not.toHaveBeenCalled();
    expect(second.close).toHaveBeenCalledOnce();
    expect(second.listenerCount("data")).toBe(0);
  });

  it("settings payloads retain wrapper, channel, timestamp and GET-opcode correlation", async () => {
    const session = peer();
    const { router } = harness([session]);
    const pending = router.getAutoLockState(DEVICE_SN, identity);
    await vi.advanceTimersByTimeAsync(0);
    const payload = session.sendControlLevel2.mock.calls[0]![3] as unknown as { time: number };
    const plain = Buffer.from([0, 0xa1, 1, 1, 0xa2, 2, 30, 0, 0xa3, 1, 0, 0xa4, 2, 0, 0, 0xa5, 2, 0, 0]);
    const good = buildFf09ResponseFrame(plain, payload.time, ACCOUNT_ID, DEVICE_SN);
    const badOpcode = Buffer.from(good, "hex");
    badOpcode[8] = 0x34;
    const frame = {
      channel: 1,
      commandId: 1351,
      json: { cmd: 1940, payload: { time: payload.time, lock_payload: good } },
    };
    for (const wrong of [
      { ...frame, channel: 0 },
      { ...frame, commandId: 1350 },
      { ...frame, json: { cmd: 1940, payload: { time: payload.time + 1, lock_payload: good } } },
      { ...frame, json: { cmd: 1940, payload: { time: payload.time, lock_payload: badOpcode.toString("hex") } } },
    ])
      session.emit("data", wrong);
    expect(session.listenerCount("data")).toBe(1);
    session.emit("data", frame);
    await expect(pending).resolves.toMatchObject({ enabled: true, delaySeconds: 30 });
  });

  it("a malformed timestamp-matched GET payload rejects as a protocol error", async () => {
    const session = peer();
    const { router } = harness([session]);
    const pending = router.getAutoLockState(DEVICE_SN, identity);
    const assertion = expect(pending).rejects.toMatchObject({ reason: "invalid-response" });
    await vi.advanceTimersByTimeAsync(0);
    const payload = session.sendControlLevel2.mock.calls[0]![3] as unknown as { time: number };
    session.emit("data", {
      channel: 1,
      commandId: 1351,
      json: { cmd: 1940, payload: { time: payload.time, lock_payload: "ff09" } },
    });
    await assertion;
    expect(session.close).toHaveBeenCalledOnce();
  });

  it("repeated successful phases leave no lifetime abort listeners", async () => {
    const sessions = Array.from({ length: 12 }, () => peer());
    const { router } = harness(sessions);
    const lifetime = (router as unknown as { commandLifetime: AbortController }).commandLifetime.signal;
    for (const session of sessions) {
      const pending = actuate(router);
      await vi.advanceTimersByTimeAsync(0);
      session.emit("commandResult", result(0));
      await pending;
      expect(getEventListeners(lifetime, "abort")).toHaveLength(0);
    }
  });

  it("constructs a fresh private session from the endpoint without manager registration or fan-out", async () => {
    const main = peer();
    const onFrame = vi.fn();
    const router = routerWithSession(main, {
      deps: {
        mega: { getDskKeys: vi.fn(async () => ({})) } as any,
        listDevices: () =>
          [
            { sn: DEVICE_SN, stationSn: STATION_SN, channel: 1, raw: { device_channel: 1 } },
            { sn: STATION_SN, p2pDid: "XXXXXXX-000000-XXXXX", raw: {} },
          ] as any,
        onFrame,
      },
    });
    const construct = (
      router as unknown as { createSession(parent: string, key: string, announces: boolean): Promise<P2PSession> }
    ).createSession.bind(router);
    const first = await construct(STATION_SN, "", false);
    const second = await construct(STATION_SN, "", false);
    expect(first).not.toBe(second);
    first.emit("data", { channel: 1 });
    expect(onFrame).not.toHaveBeenCalled();
    await first.close();
    await second.close();
    expect(main.close).not.toHaveBeenCalled();
  });
});
