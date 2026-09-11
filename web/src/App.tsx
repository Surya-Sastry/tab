import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type MouseEvent,
  type ReactNode,
} from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import {
  Activity,
  ArrowDownRight,
  ArrowRight,
  ArrowUpRight,
  Check,
  ChevronRight,
  CircleDollarSign,
  Clock3,
  LogOut,
  Menu,
  Plus,
  ReceiptText,
  Sparkles,
  Users,
  WalletCards,
  X,
} from "lucide-react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { ApiError, formatMoney, TabApi, toMinorUnits, toNonNegativeMinorUnits } from "./api";
import { useAuth } from "./auth";
import type { Group, GroupSnapshot, Member, Transfer, User } from "./types";

type ModalName = "group" | "member" | "expense" | null;
type ViewName = "overview" | "expenses" | "activity";

const emptySnapshot: GroupSnapshot = {
  members: [],
  expenses: [],
  balances: [],
  suggestions: [],
  activity: [],
};

export function App() {
  const { token } = useAuth();
  return (
    <Routes>
      <Route path="/login" element={token ? <Navigate to="/" replace /> : <AuthScreen />} />
      <Route path="/" element={token ? <Dashboard /> : <Navigate to="/login" replace />} />
      <Route path="*" element={<Navigate to={token ? "/" : "/login"} replace />} />
    </Routes>
  );
}

function AuthScreen() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);
  const anonymousApi = useMemo(() => new TabApi(() => null), []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError("");
    setPending(true);
    try {
      if (mode === "register") await anonymousApi.register(name, email, password);
      await login(email, password);
      navigate("/");
    } catch (reason) {
      setError(messageOf(reason));
    } finally {
      setPending(false);
    }
  }

  return (
    <main className="auth-layout">
      <Ambient />
      <motion.section
        className="auth-story"
        initial={{ opacity: 0, x: -28 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.7 }}
      >
        <Brand />
        <div className="eyebrow"><Sparkles size={14} /> Shared money, without the fog</div>
        <h1>Every expense.<br /><span>Perfectly clear.</span></h1>
        <p>
          A calm, event-driven place for groups to record expenses, follow balances,
          and settle up without losing the story behind the numbers.
        </p>
        <div className="story-stats">
          <MiniStat value="Exact" label="integer accounting" />
          <MiniStat value="Live" label="balance projections" />
          <MiniStat value="Safe" label="duplicate handling" />
        </div>
      </motion.section>

      <motion.section
        className="auth-panel glass"
        initial={{ opacity: 0, y: 24, scale: 0.98 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.55, delay: 0.1 }}
      >
        <div className="auth-tabs" role="tablist" aria-label="Authentication mode">
          {(["login", "register"] as const).map((item) => (
            <button
              className={mode === item ? "active" : ""}
              key={item}
              onClick={() => {
                setMode(item);
                setError("");
              }}
              role="tab"
              aria-selected={mode === item}
              type="button"
            >
              {item === "login" ? "Sign in" : "Create account"}
            </button>
          ))}
        </div>
        <div className="auth-heading">
          <span className="kicker">{mode === "login" ? "Welcome back" : "Start a new tab"}</span>
          <h2>{mode === "login" ? "Pick up where you left off." : "Make shared costs simple."}</h2>
        </div>
        <form onSubmit={submit} className="form-stack">
          {mode === "register" && (
            <Field label="Your name">
              <input value={name} onChange={(event) => setName(event.target.value)} required maxLength={120} />
            </Field>
          )}
          <Field label="Email">
            <input value={email} onChange={(event) => setEmail(event.target.value)} required type="email" autoComplete="email" />
          </Field>
          <Field label="Password" hint="12 characters minimum">
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              required
              minLength={12}
              maxLength={256}
              type="password"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
            />
          </Field>
          {error && <Notice tone="error">{error}</Notice>}
          <button className="primary-button wide" disabled={pending}>
            {pending ? <Spinner /> : mode === "login" ? "Enter Tab" : "Create and enter"}
            {!pending && <ArrowRight size={17} />}
          </button>
        </form>
        <p className="privacy-note">Your session stays in this tab and disappears when you refresh.</p>
      </motion.section>
    </main>
  );
}

function Dashboard() {
  const { api, user, logout } = useAuth();
  const reducedMotion = useReducedMotion();
  const [groups, setGroups] = useState<Group[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [snapshot, setSnapshot] = useState<GroupSnapshot>(emptySnapshot);
  const [view, setView] = useState<ViewName>("overview");
  const [modal, setModal] = useState<ModalName>(null);
  const [loadingGroups, setLoadingGroups] = useState(true);
  const [loadingSnapshot, setLoadingSnapshot] = useState(false);
  const [error, setError] = useState("");
  const [mobileNav, setMobileNav] = useState(false);
  const pollGeneration = useRef(0);
  const selected = groups.find((group) => group.id === selectedId) ?? null;

  const loadGroups = useCallback(async () => {
    setLoadingGroups(true);
    try {
      const next = await api.groups();
      setGroups(next);
      setSelectedId((current) => current || next[0]?.id || "");
      setError("");
    } catch (reason) {
      setError(messageOf(reason));
    } finally {
      setLoadingGroups(false);
    }
  }, [api]);

  const refresh = useCallback(
    async (quiet = false) => {
      if (!selectedId) {
        setSnapshot(emptySnapshot);
        return;
      }
      if (!quiet) setLoadingSnapshot(true);
      try {
        const [members, expenses, balances, suggestions, activity] = await Promise.all([
          api.members(selectedId),
          api.expenses(selectedId),
          api.balances(selectedId),
          api.suggestions(selectedId),
          api.activity(selectedId),
        ]);
        setSnapshot({ members, expenses, balances, suggestions, activity });
        setError("");
      } catch (reason) {
        if (!(reason instanceof DOMException && reason.name === "AbortError")) {
          setError(messageOf(reason));
        }
      } finally {
        if (!quiet) setLoadingSnapshot(false);
      }
    },
    [api, selectedId],
  );

  const burstRefresh = useCallback(async () => {
    const generation = ++pollGeneration.current;
    for (const delay of [200, 650, 1400, 2800, 5000]) {
      await new Promise((resolve) => window.setTimeout(resolve, delay));
      if (generation !== pollGeneration.current || document.hidden) return;
      await refresh(true);
    }
  }, [refresh]);

  useEffect(() => void loadGroups(), [loadGroups]);
  useEffect(() => {
    pollGeneration.current++;
    void refresh();
  }, [refresh]);
  useEffect(() => {
    const interval = window.setInterval(() => {
      if (!document.hidden) void refresh(true);
    }, 15_000);
    return () => window.clearInterval(interval);
  }, [refresh]);

  function pointerGlow(event: MouseEvent<HTMLDivElement>) {
    if (reducedMotion) return;
    const target = event.currentTarget;
    target.style.setProperty("--pointer-x", `${event.clientX}px`);
    target.style.setProperty("--pointer-y", `${event.clientY}px`);
  }

  async function afterMutation(refreshGroups = false) {
    if (refreshGroups) await loadGroups();
    await refresh();
    void burstRefresh();
    setModal(null);
  }

  return (
    <div className="app-shell" onMouseMove={pointerGlow}>
      <Ambient />
      <aside className={`sidebar glass ${mobileNav ? "open" : ""}`}>
        <Brand compact />
        <nav aria-label="Main navigation">
          <NavButton active={view === "overview"} icon={<WalletCards />} onClick={() => setView("overview")}>Overview</NavButton>
          <NavButton active={view === "expenses"} icon={<ReceiptText />} onClick={() => setView("expenses")}>Expenses</NavButton>
          <NavButton active={view === "activity"} icon={<Activity />} onClick={() => setView("activity")}>Activity</NavButton>
        </nav>
        <div className="sidebar-foot">
          <div className="avatar">{initials(user?.name ?? "T")}</div>
          <div><strong>{user?.name}</strong><span>{user?.email}</span></div>
          <button className="icon-button" onClick={logout} aria-label="Sign out"><LogOut size={18} /></button>
        </div>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <button className="icon-button mobile-menu" onClick={() => setMobileNav((open) => !open)} aria-label="Toggle navigation">
            <Menu />
          </button>
          <div>
            <span className="kicker">Your shared spaces</span>
            <h2>Good evening, {firstName(user?.name)}</h2>
          </div>
          <button className="primary-button" onClick={() => setModal("expense")} disabled={!selected}>
            <Plus size={17} /> Add expense
          </button>
        </header>

        <section className="group-rail" aria-label="Groups">
          <button className="group-card add-card" onClick={() => setModal("group")}>
            <span><Plus /></span><strong>New group</strong><small>Start a shared space</small>
          </button>
          {loadingGroups ? (
            <><Skeleton className="group-card" /><Skeleton className="group-card" /></>
          ) : (
            groups.map((group, index) => (
              <motion.button
                key={group.id}
                className={`group-card ${selectedId === group.id ? "selected" : ""}`}
                onClick={() => setSelectedId(group.id)}
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.06 }}
              >
                <span className={`group-orb tone-${index % 4}`}>{initials(group.name)}</span>
                <strong>{group.name}</strong>
                <small>{group.currency} · shared ledger</small>
              </motion.button>
            ))
          )}
        </section>

        {error && <Notice tone="error">{error}</Notice>}
        {!loadingGroups && groups.length === 0 ? (
          <EmptyState
            icon={<Users />}
            title="Your first group starts here"
            copy="Create a space for a trip, a home, or any circle that shares expenses."
            action="Create group"
            onAction={() => setModal("group")}
          />
        ) : selected ? (
          <AnimatePresence mode="wait">
            <motion.div
              key={`${selected.id}-${view}`}
              initial={{ opacity: 0, y: 14 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.28 }}
            >
              <GroupHero group={selected} snapshot={snapshot} user={user} onMember={() => setModal("member")} />
              {loadingSnapshot ? (
                <DashboardSkeleton />
              ) : view === "overview" ? (
                <Overview group={selected} snapshot={snapshot} user={user} api={api} afterMutation={afterMutation} />
              ) : view === "expenses" ? (
                <ExpenseList group={selected} snapshot={snapshot} />
              ) : (
                <ActivityTimeline group={selected} snapshot={snapshot} />
              )}
            </motion.div>
          </AnimatePresence>
        ) : null}
      </main>

      <AnimatePresence>
        {modal === "group" && <CreateGroupModal api={api} onClose={() => setModal(null)} onDone={() => afterMutation(true)} />}
        {modal === "member" && selected && <AddMemberModal api={api} group={selected} onClose={() => setModal(null)} onDone={() => afterMutation()} />}
        {modal === "expense" && selected && (
          <CreateExpenseModal api={api} group={selected} members={snapshot.members} user={user} onClose={() => setModal(null)} onDone={() => afterMutation()} />
        )}
      </AnimatePresence>
    </div>
  );
}

function GroupHero({ group, snapshot, user, onMember }: { group: Group; snapshot: GroupSnapshot; user: User | null; onMember: () => void }) {
  const own = snapshot.balances.find((balance) => balance.userId === user?.id)?.amountMinor ?? 0;
  return (
    <section className="group-hero glass">
      <div className="hero-copy">
        <span className="eyebrow"><CircleDollarSign size={14} /> Live group ledger</span>
        <h1>{group.name}</h1>
        <p>{snapshot.members.length} members · {snapshot.expenses.length} expenses · balances update asynchronously</p>
        <div className="hero-actions">
          <button className="secondary-button" onClick={onMember}><Users size={16} /> Add member</button>
          <span className={`status-pill ${own < 0 ? "negative" : own > 0 ? "positive" : ""}`}>
            {own === 0 ? "You're settled" : own > 0 ? `${formatMoney(own, group.currency)} owed to you` : `${formatMoney(-own, group.currency)} you owe`}
          </span>
        </div>
      </div>
      <div className="hero-visual" aria-hidden="true">
        <div className="orbit orbit-one" /><div className="orbit orbit-two" />
        <div className="hero-coin">{group.currency}</div>
      </div>
    </section>
  );
}

function Overview({ group, snapshot, user, api, afterMutation }: { group: Group; snapshot: GroupSnapshot; user: User | null; api: TabApi; afterMutation: () => Promise<void> }) {
  const memberMap = useMemo(() => new Map(snapshot.members.map((member) => [member.id, member])), [snapshot.members]);
  const own = snapshot.balances.find((balance) => balance.userId === user?.id)?.amountMinor ?? 0;
  const total = snapshot.expenses.filter((item) => item.status === "ACTIVE").reduce((sum, item) => sum + item.amountMinor, 0);
  return (
    <>
      <section className="metric-grid">
        <MetricCard icon={<ArrowUpRight />} label="Owed to you" value={formatMoney(Math.max(own, 0), group.currency)} tone="mint" />
        <MetricCard icon={<ArrowDownRight />} label="You owe" value={formatMoney(Math.max(-own, 0), group.currency)} tone="coral" />
        <MetricCard icon={<ReceiptText />} label="Group spend" value={formatMoney(total, group.currency)} tone="violet" />
      </section>
      <section className="content-grid">
        <Panel title="Balance board" subtitle="Derived from the event stream" action={<span className="live-dot">Live</span>}>
          <div className="balance-list">
            {snapshot.balances.length ? snapshot.balances.map((balance, index) => (
              <motion.div className="balance-row" key={balance.userId} initial={{ opacity: 0, x: -10 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: index * 0.05 }}>
                <div className="avatar soft">{initials(memberMap.get(balance.userId)?.name ?? "?")}</div>
                <div className="grow"><strong>{memberMap.get(balance.userId)?.name ?? shortId(balance.userId)}</strong><span>{balance.userId === user?.id ? "You" : "Group member"}</span></div>
                <strong className={balance.amountMinor < 0 ? "money negative" : balance.amountMinor > 0 ? "money positive" : "money"}>
                  {balance.amountMinor > 0 ? "+" : ""}{formatMoney(balance.amountMinor, group.currency)}
                </strong>
              </motion.div>
            )) : <EmptyInline copy="No balance movement yet." />}
          </div>
        </Panel>
        <Panel title="Settle smarter" subtitle="Suggested transfers, not ledger entries" action={<Sparkles size={18} />}>
          <div className="suggestion-list">
            {snapshot.suggestions.length ? snapshot.suggestions.map((transfer) => (
              <SettlementSuggestion key={`${transfer.fromUserId}-${transfer.toUserId}`} transfer={transfer} group={group} members={memberMap} api={api} afterMutation={afterMutation} user={user} />
            )) : <EmptyInline copy="Everyone is balanced. Nice." />}
          </div>
        </Panel>
      </section>
      <Panel title="Recent movement" subtitle="The latest group events" action={<Clock3 size={18} />}>
        <ActivityRows group={group} snapshot={snapshot} memberMap={memberMap} limit={4} />
      </Panel>
    </>
  );
}

function ExpenseList({ group, snapshot }: { group: Group; snapshot: GroupSnapshot }) {
  const memberMap = new Map(snapshot.members.map((member) => [member.id, member]));
  return (
    <Panel title="Expense history" subtitle="Append-only financial story" action={<span>{snapshot.expenses.length} records</span>}>
      <div className="expense-list">
        {snapshot.expenses.length ? snapshot.expenses.map((expense, index) => (
          <motion.article className={`expense-row ${expense.status === "VOIDED" ? "voided" : ""}`} key={expense.id} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: index * 0.04 }}>
            <div className="expense-icon"><ReceiptText size={19} /></div>
            <div className="grow"><strong>{expense.description}</strong><span>Paid by {memberMap.get(expense.payerId)?.name ?? shortId(expense.payerId)} · {expense.splitStrategy.toLowerCase()} split</span></div>
            <div className="expense-amount"><strong>{formatMoney(expense.amountMinor, group.currency)}</strong><span>{expense.status.toLowerCase()}</span></div>
          </motion.article>
        )) : <EmptyInline copy="Add the first expense to start the ledger." />}
      </div>
    </Panel>
  );
}

function ActivityTimeline({ group, snapshot }: { group: Group; snapshot: GroupSnapshot }) {
  return (
    <Panel title="Activity timeline" subtitle="A rebuildable view of every event" action={<Activity size={18} />}>
      <ActivityRows group={group} snapshot={snapshot} memberMap={new Map(snapshot.members.map((member) => [member.id, member]))} />
    </Panel>
  );
}

function ActivityRows({ group, snapshot, memberMap, limit }: { group: Group; snapshot: GroupSnapshot; memberMap: Map<string, Member>; limit?: number }) {
  const items = limit ? snapshot.activity.slice(0, limit) : snapshot.activity;
  return (
    <div className="timeline">
      {items.length ? items.map((item, index) => (
        <div className="timeline-item" key={item.eventId}>
          <span className="timeline-node">{index === 0 ? <Sparkles size={14} /> : <Check size={14} />}</span>
          <div className="grow"><strong>{activityLabel(item.type, item.summary)}</strong><span>{memberMap.get(item.actorId)?.name ?? "A group member"} · {relativeTime(item.createdAt)}</span></div>
          {item.amountMinor > 0 && <strong>{formatMoney(item.amountMinor, group.currency)}</strong>}
        </div>
      )) : <EmptyInline copy="Activity will appear as events are projected." />}
    </div>
  );
}

function SettlementSuggestion({ transfer, group, members, api, afterMutation, user }: { transfer: Transfer; group: Group; members: Map<string, Member>; api: TabApi; afterMutation: () => Promise<void>; user: User | null }) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function settle() {
    setPending(true);
    setError("");
    try {
      await api.settle(group.id, transfer);
      await afterMutation();
    } catch (reason) {
      setError(messageOf(reason));
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="suggestion">
      <div className="transfer-avatars">
        <span>{initials(members.get(transfer.fromUserId)?.name ?? "?")}</span>
        <ArrowRight size={15} />
        <span>{initials(members.get(transfer.toUserId)?.name ?? "?")}</span>
      </div>
      <div className="grow"><strong>{members.get(transfer.fromUserId)?.name ?? shortId(transfer.fromUserId)} → {members.get(transfer.toUserId)?.name ?? shortId(transfer.toUserId)}</strong><span>{formatMoney(transfer.amountMinor, group.currency)}</span>{error && <small className="error-text">{error}</small>}</div>
      {transfer.fromUserId === user?.id && <button className="mini-button" disabled={pending} onClick={settle}>{pending ? "Saving…" : "Record paid"}</button>}
    </div>
  );
}

function CreateGroupModal({ api, onClose, onDone }: { api: TabApi; onClose: () => void; onDone: () => Promise<void> }) {
  const [name, setName] = useState("");
  const [currency, setCurrency] = useState("INR");
  return (
    <Modal title="Create a group" subtitle="A private ledger for your circle" onClose={onClose}>
      <AsyncForm action={async () => { await api.createGroup(name, currency); await onDone(); }} submit="Create group">
        <Field label="Group name"><input value={name} onChange={(event) => setName(event.target.value)} required maxLength={120} autoFocus /></Field>
        <Field label="Currency">
          <select value={currency} onChange={(event) => setCurrency(event.target.value)}>
            <option value="INR">INR — Indian rupee</option><option value="USD">USD — US dollar</option><option value="EUR">EUR — Euro</option><option value="GBP">GBP — British pound</option>
          </select>
        </Field>
      </AsyncForm>
    </Modal>
  );
}

function AddMemberModal({ api, group, onClose, onDone }: { api: TabApi; group: Group; onClose: () => void; onDone: () => Promise<void> }) {
  const [userId, setUserId] = useState("");
  return (
    <Modal title="Add a member" subtitle={`Invite an existing user to ${group.name}`} onClose={onClose}>
      <AsyncForm action={async () => { await api.addMember(group.id, userId.trim()); await onDone(); }} submit="Add member">
        <Field label="User ID" hint="Use the UUID returned when that user registered"><input value={userId} onChange={(event) => setUserId(event.target.value)} required pattern="[0-9a-fA-F-]{36}" autoFocus /></Field>
      </AsyncForm>
    </Modal>
  );
}

function CreateExpenseModal({ api, group, members, user, onClose, onDone }: { api: TabApi; group: Group; members: Member[]; user: User | null; onClose: () => void; onDone: () => Promise<void> }) {
  const [description, setDescription] = useState("");
  const [amount, setAmount] = useState("");
  const [payerId, setPayerId] = useState(user?.id ?? members[0]?.id ?? "");
  const [strategy, setStrategy] = useState<"EQUAL" | "EXACT">("EQUAL");
  const [participants, setParticipants] = useState<string[]>(members.map((member) => member.id));
  const [exact, setExact] = useState<Record<string, string>>({});

  async function create() {
    const amountMinor = toMinorUnits(amount);
    await api.createExpense(group.id, {
      payerId,
      description,
      amountMinor,
      splitStrategy: strategy,
      participantIds: strategy === "EQUAL" ? participants : undefined,
      splits: strategy === "EXACT"
        ? participants.map((userId) => ({ userId, amountMinor: toNonNegativeMinorUnits(exact[userId] || "0") }))
        : undefined,
    });
    await onDone();
  }

  function toggle(id: string) {
    setParticipants((current) => current.includes(id) ? current.filter((item) => item !== id) : [...current, id]);
  }

  return (
    <Modal title="Add an expense" subtitle={`Post directly to ${group.name}'s ledger`} onClose={onClose} wide>
      <AsyncForm action={create} submit="Add to ledger">
        <div className="form-grid">
          <Field label="Description"><input value={description} onChange={(event) => setDescription(event.target.value)} required maxLength={500} autoFocus /></Field>
          <Field label={`Amount (${group.currency})`}><input value={amount} onChange={(event) => setAmount(event.target.value)} required inputMode="decimal" placeholder="0.00" /></Field>
        </div>
        <Field label="Paid by">
          <select value={payerId} onChange={(event) => setPayerId(event.target.value)} required>
            {members.map((member) => <option value={member.id} key={member.id}>{member.name}</option>)}
          </select>
        </Field>
        <div className="segmented">
          <button type="button" className={strategy === "EQUAL" ? "active" : ""} onClick={() => setStrategy("EQUAL")}>Split equally</button>
          <button type="button" className={strategy === "EXACT" ? "active" : ""} onClick={() => setStrategy("EXACT")}>Exact amounts</button>
        </div>
        <div className="member-picker">
          {members.map((member) => {
            const checked = participants.includes(member.id);
            return (
              <div className={`member-option ${checked ? "checked" : ""}`} key={member.id}>
                <button type="button" onClick={() => toggle(member.id)} aria-pressed={checked}>
                  <span className="check-box">{checked && <Check size={14} />}</span>
                  <span>{member.name}</span>
                </button>
                {strategy === "EXACT" && checked && (
                  <input aria-label={`${member.name} amount`} value={exact[member.id] ?? ""} onChange={(event) => setExact((current) => ({ ...current, [member.id]: event.target.value }))} inputMode="decimal" placeholder="0.00" />
                )}
              </div>
            );
          })}
        </div>
      </AsyncForm>
    </Modal>
  );
}

function AsyncForm({ action, submit, children }: { action: () => Promise<void>; submit: string; children: ReactNode }) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setError("");
    setPending(true);
    try {
      await action();
    } catch (reason) {
      setError(messageOf(reason));
    } finally {
      setPending(false);
    }
  }
  return (
    <form onSubmit={onSubmit} className="form-stack">
      {children}
      {error && <Notice tone="error">{error}</Notice>}
      <button className="primary-button wide" disabled={pending}>{pending ? <Spinner /> : submit}{!pending && <ArrowRight size={17} />}</button>
    </form>
  );
}

function Modal({ title, subtitle, onClose, wide, children }: { title: string; subtitle: string; onClose: () => void; wide?: boolean; children: ReactNode }) {
  return (
    <motion.div className="modal-backdrop" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onMouseDown={onClose}>
      <motion.section className={`modal glass ${wide ? "wide" : ""}`} role="dialog" aria-modal="true" aria-labelledby="modal-title" initial={{ opacity: 0, scale: 0.96, y: 20 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.97, y: 12 }} onMouseDown={(event) => event.stopPropagation()}>
        <button className="icon-button modal-close" onClick={onClose} aria-label="Close"><X /></button>
        <span className="kicker">Tab workspace</span><h2 id="modal-title">{title}</h2><p>{subtitle}</p>
        {children}
      </motion.section>
    </motion.div>
  );
}

function Panel({ title, subtitle, action, children }: { title: string; subtitle: string; action?: ReactNode; children: ReactNode }) {
  return <section className="panel glass"><header><div><h3>{title}</h3><p>{subtitle}</p></div><div className="panel-action">{action}</div></header>{children}</section>;
}

function MetricCard({ icon, label, value, tone }: { icon: ReactNode; label: string; value: string; tone: string }) {
  return <motion.article className={`metric-card glass ${tone}`} whileHover={{ y: -5, scale: 1.01 }} transition={{ type: "spring", stiffness: 300 }}><span className="metric-icon">{icon}</span><div><span>{label}</span><strong>{value}</strong></div><ChevronRight size={18} /></motion.article>;
}

function EmptyState({ icon, title, copy, action, onAction }: { icon: ReactNode; title: string; copy: string; action: string; onAction: () => void }) {
  return <motion.section className="empty-state glass" initial={{ opacity: 0, scale: 0.98 }} animate={{ opacity: 1, scale: 1 }}><span>{icon}</span><h2>{title}</h2><p>{copy}</p><button className="primary-button" onClick={onAction}><Plus size={17} /> {action}</button></motion.section>;
}

function EmptyInline({ copy }: { copy: string }) { return <div className="empty-inline"><Sparkles size={18} /><span>{copy}</span></div>; }
function Field({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) { return <label className="field"><span>{label}{hint && <small>{hint}</small>}</span>{children}</label>; }
function Notice({ tone, children }: { tone: "error" | "info"; children: ReactNode }) { return <div className={`notice ${tone}`}>{children}</div>; }
function Spinner() { return <span className="spinner" aria-label="Loading" />; }
function Skeleton({ className = "" }: { className?: string }) { return <div className={`skeleton ${className}`} />; }
function DashboardSkeleton() { return <><section className="metric-grid"><Skeleton className="metric-card" /><Skeleton className="metric-card" /><Skeleton className="metric-card" /></section><section className="content-grid"><Skeleton className="panel tall" /><Skeleton className="panel tall" /></section></>; }
function NavButton({ active, icon, onClick, children }: { active: boolean; icon: ReactNode; onClick: () => void; children: ReactNode }) { return <button className={active ? "active" : ""} onClick={onClick}>{icon}<span>{children}</span></button>; }
function MiniStat({ value, label }: { value: string; label: string }) { return <div><strong>{value}</strong><span>{label}</span></div>; }
function Brand({ compact = false }: { compact?: boolean }) { return <div className={`brand ${compact ? "compact" : ""}`}><span className="brand-mark"><span /></span><strong>Tab</strong>{!compact && <small>Share clearly</small>}</div>; }
function Ambient() { return <div className="ambient" aria-hidden="true"><span className="ambient-one" /><span className="ambient-two" /><span className="noise" /></div>; }
function initials(value: string) { return value.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join(""); }
function firstName(value?: string) { return value?.trim().split(/\s+/)[0] || "there"; }
function shortId(value: string) { return `${value.slice(0, 6)}…${value.slice(-4)}`; }
function messageOf(reason: unknown) { return reason instanceof ApiError || reason instanceof Error ? reason.message : "Something went wrong"; }
function activityLabel(type: string, summary: string) {
  const action = type.includes("voided") ? "Expense voided" : type.includes("updated") ? "Expense corrected" : type.includes("settlement") ? "Settlement recorded" : "Expense added";
  return summary ? `${action}: ${summary}` : action;
}
function relativeTime(value: string) {
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(new Date(value));
}