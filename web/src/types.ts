export type User = {
  id: string;
  name: string;
  email: string;
};

export type Group = {
  id: string;
  name: string;
  currency: string;
};

export type Member = {
  id: string;
  name: string;
};

export type Split = {
  userId: string;
  amountMinor: number;
};

export type Expense = {
  id: string;
  groupId: string;
  payerId: string;
  actorId: string;
  description: string;
  amountMinor: number;
  currency: string;
  splitStrategy: "EQUAL" | "EXACT";
  status: "ACTIVE" | "VOIDED";
  revision: number;
  splits?: Split[];
};

export type Balance = {
  userId: string;
  amountMinor: number;
};

export type Transfer = {
  fromUserId: string;
  toUserId: string;
  amountMinor: number;
};

export type ActivityItem = {
  eventId: string;
  groupId: string;
  type: string;
  actorId: string;
  summary: string;
  amountMinor: number;
  createdAt: string;
};

export type Settlement = {
  id: string;
  groupId: string;
  fromUserId: string;
  toUserId: string;
  actorId: string;
  amountMinor: number;
  status: "RECORDED" | "REVERSED";
};

export type GroupSnapshot = {
  members: Member[];
  expenses: Expense[];
  balances: Balance[];
  suggestions: Transfer[];
  activity: ActivityItem[];
};
