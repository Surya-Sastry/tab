import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, formatMoney, TabApi, toMinorUnits } from "./api";

afterEach(() => vi.restoreAllMocks());

describe("money helpers", () => {
  it("converts decimal input to integer minor units", () => {
    expect(toMinorUnits("100.01")).toBe(10_001);
    expect(toMinorUnits("1.5")).toBe(150);
  });

  it("rejects unsafe or over-precise amounts", () => {
    expect(() => toMinorUnits("1.001")).toThrow();
    expect(() => toMinorUnits("-1")).toThrow();
  });

  it("formats minor units in the group currency", () => {
    expect(formatMoney(10_001, "INR")).toMatch(/100\.01/);
  });
});

describe("TabApi", () => {
  it("adds authorization and idempotency headers to mutations", async () => {
    vi.stubGlobal("crypto", { randomUUID: () => "request-id" });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ id: "expense-1" }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const api = new TabApi(() => "memory-token");
    await api.createExpense("group-1", {
      payerId: "user-1",
      description: "Dinner",
      amountMinor: 100,
      splitStrategy: "EQUAL",
      participantIds: ["user-1"],
    });

    const [, options] = fetchMock.mock.calls[0];
    const headers = options?.headers as Headers;
    expect(headers.get("Authorization")).toBe("Bearer memory-token");
    expect(headers.get("Idempotency-Key")).toBe("request-id");
  });

  it("surfaces the backend error envelope", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ error: "split sum does not equal total" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const api = new TabApi(() => null);
    await expect(api.groups()).rejects.toEqual(
      new ApiError("split sum does not equal total", 400),
    );
  });
});
