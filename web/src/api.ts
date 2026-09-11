import type {
  ActivityItem,
  Balance,
  Expense,
  Group,
  Member,
  Settlement,
  Split,
  Transfer,
  User,
} from "./types";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

type RequestOptions = {
  method?: "GET" | "POST" | "PATCH" | "DELETE";
  body?: unknown;
  idempotent?: boolean;
  signal?: AbortSignal;
};

export class TabApi {
  constructor(private readonly getToken: () => string | null) {}

  private async request<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const headers = new Headers({ Accept: "application/json" });
    const token = this.getToken();
    if (token) headers.set("Authorization", `Bearer ${token}`);
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    if (options.idempotent) headers.set("Idempotency-Key", crypto.randomUUID());

    const response = await fetch(path, {
      method: options.method ?? "GET",
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: options.signal,
    });
    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as { error?: string } | null;
      throw new ApiError(payload?.error ?? `Request failed (${response.status})`, response.status);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  register(name: string, email: string, password: string) {
    return this.request<User>("/api/v1/auth/register", {
      method: "POST",
      body: { Name: name, Email: email, Password: password },
    });
  }

  login(email: string, password: string) {
    return this.request<{ accessToken: string; expiresIn: number }>("/api/v1/auth/login", {
      method: "POST",
      body: { Email: email, Password: password },
    });
  }

  me(signal?: AbortSignal) {
    return this.request<User>("/api/v1/auth/me", { signal });
  }

  groups(signal?: AbortSignal) {
    return this.request<Group[] | null>("/api/v1/groups", { signal }).then((value) => value ?? []);
  }

  createGroup(name: string, currency: string) {
    return this.request<Group>("/api/v1/groups", {
      method: "POST",
      body: { Name: name, Currency: currency },
    });
  }

  members(groupId: string, signal?: AbortSignal) {
    return this.request<Member[] | null>(`/api/v1/groups/${groupId}/members`, { signal }).then(
      (value) => value ?? [],
    );
  }

  addMember(groupId: string, userId: string) {
    return this.request<void>(`/api/v1/groups/${groupId}/members`, {
      method: "POST",
      body: { userId },
    });
  }

  expenses(groupId: string, signal?: AbortSignal) {
    return this.request<Expense[] | null>(`/api/v1/groups/${groupId}/expenses`, {
      signal,
    }).then((value) => value ?? []);
  }

  createExpense(
    groupId: string,
    body: {
      payerId: string;
      description: string;
      amountMinor: number;
      splitStrategy: "EQUAL" | "EXACT";
      participantIds?: string[];
      splits?: Split[];
    },
  ) {
    return this.request<Expense>(`/api/v1/groups/${groupId}/expenses`, {
      method: "POST",
      body,
      idempotent: true,
    });
  }

  balances(groupId: string, signal?: AbortSignal) {
    return this.request<Balance[] | null>(`/api/v1/groups/${groupId}/balances`, {
      signal,
    }).then((value) => value ?? []);
  }

  suggestions(groupId: string, signal?: AbortSignal) {
    return this.request<Transfer[] | null>(
      `/api/v1/groups/${groupId}/suggested-settlements`,
      { signal },
    ).then((value) => value ?? []);
  }

  activity(groupId: string, signal?: AbortSignal) {
    return this.request<ActivityItem[] | null>(`/api/v1/groups/${groupId}/activity`, {
      signal,
    }).then((value) => value ?? []);
  }

  settle(groupId: string, transfer: Transfer) {
    return this.request<Settlement>(`/api/v1/groups/${groupId}/settlements`, {
      method: "POST",
      body: transfer,
      idempotent: true,
    });
  }
}

export function toMinorUnits(value: string): number {
  const minor = parseMinorUnits(value);
  if (minor <= 0) throw new Error("Enter a valid positive amount");
  return minor;
}

export function toNonNegativeMinorUnits(value: string): number {
  return parseMinorUnits(value);
}

function parseMinorUnits(value: string): number {
  const normalized = value.trim();
  if (!/^\d+(\.\d{1,2})?$/.test(normalized)) throw new Error("Enter a valid amount");
  const [whole, fraction = ""] = normalized.split(".");
  const minor = Number(whole) * 100 + Number(fraction.padEnd(2, "0"));
  if (!Number.isSafeInteger(minor) || minor < 0) throw new Error("Enter a valid amount");
  return minor;
}

export function formatMoney(amountMinor: number, currency: string): string {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(amountMinor / 100);
}
