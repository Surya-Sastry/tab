import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import { App } from "./App";
import { AuthProvider } from "./auth";

function renderApp() {
  return render(
    <MemoryRouter initialEntries={["/login"]}>
      <AuthProvider>
        <App />
      </AuthProvider>
    </MemoryRouter>,
  );
}

describe("authentication screen", () => {
  it("starts in sign-in mode without persisting a token", () => {
    renderApp();
    expect(screen.getByRole("heading", { name: /pick up where you left off/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /enter tab/i })).toBeEnabled();
  });

  it("switches to account creation fields", () => {
    renderApp();
    fireEvent.click(screen.getByRole("tab", { name: /create account/i }));
    expect(screen.getByLabelText(/your name/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /create and enter/i })).toBeEnabled();
  });
});
