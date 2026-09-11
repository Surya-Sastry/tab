import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { TabApi } from "./api";
import type { User } from "./types";

type AuthValue = {
  api: TabApi;
  user: User | null;
  token: string | null;
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
};

const AuthContext = createContext<AuthValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [expiresAt, setExpiresAt] = useState<number | null>(null);
  const api = useMemo(() => new TabApi(() => token), [token]);

  const logout = useCallback(() => {
    setToken(null);
    setUser(null);
    setExpiresAt(null);
  }, []);

  const login = useCallback(async (email: string, password: string) => {
    const anonymous = new TabApi(() => null);
    const result = await anonymous.login(email, password);
    const authenticated = new TabApi(() => result.accessToken);
    const currentUser = await authenticated.me();
    setToken(result.accessToken);
    setUser(currentUser);
    setExpiresAt(Date.now() + result.expiresIn * 1000);
  }, []);

  useEffect(() => {
    if (!expiresAt) return;
    const timer = window.setTimeout(logout, Math.max(0, expiresAt - Date.now()));
    return () => window.clearTimeout(timer);
  }, [expiresAt, logout]);

  const value = useMemo(
    () => ({ api, user, token, login, logout }),
    [api, user, token, login, logout],
  );
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// Provider and hook intentionally share this small module.
// eslint-disable-next-line react-refresh/only-export-components
export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) throw new Error("useAuth must be used inside AuthProvider");
  return context;
}
