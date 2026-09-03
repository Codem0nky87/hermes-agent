// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api, type MessagingPlatform } from "@/lib/api";
import ChannelsPage from "./ChannelsPage";

const hookMocks = vi.hoisted(() => ({
  setEnd: vi.fn(),
  showToast: vi.fn(),
}));

// ESM namespaces are not configurable, so the QR encoder is mocked rather
// than spied. Telegram still uses it; WhatsApp must never reach it.
const qrMocks = vi.hoisted(() => ({
  toDataURL: vi.fn(async () => "data:image/png;base64,FAKEQRIMAGECANARY"),
}));
vi.mock("qrcode", () => ({
  toDataURL: qrMocks.toDataURL,
  default: { toDataURL: qrMocks.toDataURL },
}));

vi.mock("@/contexts/usePageHeader", () => ({
  usePageHeader: () => ({ setEnd: hookMocks.setEnd }),
}));
vi.mock("@/hooks/useModalBehavior", () => ({
  useModalBehavior: () => null,
}));
vi.mock("@nous-research/ui/hooks/use-toast", () => ({
  useToast: () => ({ toast: null, showToast: hookMocks.showToast }),
}));

const confirmationRequired = new Error(
  '409: {"detail":"WhatsApp session reset requires confirmation."}',
);
const whatsappPlatform: MessagingPlatform = {
  id: "whatsapp",
  name: "WhatsApp",
  description: "WhatsApp test channel",
  docs_url: "",
  enabled: false,
  configured: true,
  gateway_running: false,
  state: "disabled",
  error_code: null,
  error_message: null,
  updated_at: null,
  home_channel: null,
  whatsapp_setup: { mode: "bot", allowed_users_set: false },
  env_vars: [],
};

let container: HTMLDivElement;
let root: Root;

async function renderPage() {
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(async () => root.render(<ChannelsPage />));
  await vi.waitFor(() => expect(container.textContent).toContain("Check WhatsApp session"));
}

async function clickPair() {
  const pairButton = Array.from(container.querySelectorAll("button")).find(
    (button) => button.textContent?.trim() === "Check WhatsApp session",
  );
  expect(pairButton).toBeDefined();
  await act(async () => pairButton?.click());
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.spyOn(api, "getMessagingPlatforms").mockResolvedValue({
    env_path: "dashboard-env",
    gateway_start_command: "hermes gateway start",
    platforms: [whatsappPlatform],
  });
});

afterEach(async () => {
  await act(async () => root?.unmount());
  container?.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("WhatsApp revoked-session confirmation", () => {
  it("retries with destructive authorization only after confirmation", async () => {
    const confirm = vi.fn((_: string): boolean => true);
    vi.stubGlobal("confirm", confirm);
    const start = vi
      .spyOn(api, "startWhatsAppOnboarding")
      .mockRejectedValueOnce(confirmationRequired)
      .mockResolvedValueOnce({
        pairing_id: "authorized-pairing",
        status: "starting",
        expires_at: "2099-01-01T00:00:00Z",
        mode: "bot",
      });
    await renderPage();

    await clickPair();

    expect(start).toHaveBeenNthCalledWith(1, {
      mode: "bot",
      allowed_users: "",
    });
    expect(confirm).toHaveBeenCalledOnce();
    expect(confirm.mock.calls[0][0]).toContain("permanently delete");
    expect(confirm.mock.calls[0][0]).toContain("WhatsApp credentials");
    expect(confirm.mock.calls[0][0]).toContain("re-pair");
    expect(start).toHaveBeenNthCalledWith(2, {
      mode: "bot",
      allowed_users: "",
      reset_revoked_session: true,
    });
    expect(start).toHaveBeenCalledTimes(2);
  });

  it("does not retry or expose backend details when confirmation is cancelled", async () => {
    const confirm = vi.fn(() => false);
    vi.stubGlobal("confirm", confirm);
    const start = vi
      .spyOn(api, "startWhatsAppOnboarding")
      .mockRejectedValueOnce(confirmationRequired);
    await renderPage();

    await clickPair();

    expect(confirm).toHaveBeenCalledOnce();
    expect(start).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledWith({ mode: "bot", allowed_users: "" });
    expect(container.textContent).not.toContain(confirmationRequired.message);
  });
});

// ---------------------------------------------------------------------------
// repair_required is a terminal, actionable state in the client contract too.
//
// The bridge stops rather than sending pairing material to a stream the
// dashboard would store and re-serve, so the browser can never show a QR for
// it. If the page does not recognise the status it polls a finished run
// forever and shows nothing the operator can act on.
// ---------------------------------------------------------------------------

const REPAIR_MESSAGE =
  "WhatsApp needs to be paired again. Run 'hermes whatsapp' in a local terminal.";

// Shaped like a Baileys QR payload but wholly fabricated. A current server
// never sends this; an older or compromised one might, and the client must
// refuse to turn it into displayed authentication material regardless.
const HOSTILE_QR_PAYLOAD =
  "2@FAKEQRCANARY0003/SyntheticDashboardRef,FAKEPUBKEYCANARY0003=";

function expectNoWhatsAppQrRendered() {
  const images = Array.from(container.querySelectorAll("img"));
  for (const image of images) {
    expect(image.getAttribute("alt") ?? "").not.toContain("WhatsApp setup QR");
  }
  expect(container.innerHTML).not.toContain(HOSTILE_QR_PAYLOAD);
  expect(container.innerHTML).not.toContain("FAKEQRIMAGECANARY");
}

describe("WhatsApp repair_required terminal status", () => {
  it("accepts repair_required on the start response and leaves the waiting state", async () => {
    vi.spyOn(api, "startWhatsAppOnboarding").mockResolvedValue({
      pairing_id: "repair-pairing",
      status: "repair_required",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      error: REPAIR_MESSAGE,
      qr_payload: null,
    });
    const poll = vi.spyOn(api, "getWhatsAppOnboardingStatus");
    await renderPage();

    await clickPair();

    // Actionable fixed message is rendered...
    await vi.waitFor(() =>
      expect(container.textContent).toContain("hermes whatsapp"),
    );
    // ...no QR is rendered...
    expect(container.querySelector("img")).toBeNull();
    // ...and the page does not fall through to polling a finished run.
    expect(poll).not.toHaveBeenCalled();
  });

  it("stops polling and clears QR state when a poll returns repair_required", async () => {
    vi.spyOn(api, "startWhatsAppOnboarding").mockResolvedValue({
      pairing_id: "repair-pairing",
      status: "waiting",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      qr_payload: null,
    });
    const poll = vi
      .spyOn(api, "getWhatsAppOnboardingStatus")
      .mockResolvedValue({
        pairing_id: "repair-pairing",
        status: "repair_required",
        expires_at: "2099-01-01T00:00:00Z",
        mode: "bot",
        error: REPAIR_MESSAGE,
        qr_payload: null,
      });
    await renderPage();

    await clickPair();

    // The poll loop starts after a 1s delay.
    await vi.waitFor(() => expect(poll).toHaveBeenCalled(), { timeout: 4000 });
    await vi.waitFor(() =>
      expect(container.textContent).toContain("hermes whatsapp"),
    );

    const callsAfterSettle = poll.mock.calls.length;
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 2600));
    });
    expect(poll.mock.calls.length).toBe(callsAfterSettle);
    expect(container.querySelector("img")).toBeNull();
  });
});

describe("WhatsApp dashboard never renders pairing material", () => {
  it("ignores a hostile qr_payload on the start response", async () => {
    const toDataURL = qrMocks.toDataURL;
    vi.spyOn(api, "startWhatsAppOnboarding").mockResolvedValue({
      pairing_id: "hostile-start",
      status: "repair_required",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      error: REPAIR_MESSAGE,
      qr_payload: HOSTILE_QR_PAYLOAD,
    });
    const poll = vi.spyOn(api, "getWhatsAppOnboardingStatus");
    await renderPage();

    await clickPair();

    await vi.waitFor(() =>
      expect(container.textContent).toContain("hermes whatsapp"),
    );
    expectNoWhatsAppQrRendered();
    expect(toDataURL).not.toHaveBeenCalled();
    expect(poll).not.toHaveBeenCalled();
  });

  it("ignores a hostile qr_payload on a poll response", async () => {
    const toDataURL = qrMocks.toDataURL;
    vi.spyOn(api, "startWhatsAppOnboarding").mockResolvedValue({
      pairing_id: "hostile-poll",
      status: "waiting",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      qr_payload: null,
    });
    const poll = vi
      .spyOn(api, "getWhatsAppOnboardingStatus")
      .mockResolvedValue({
        pairing_id: "hostile-poll",
        status: "repair_required",
        expires_at: "2099-01-01T00:00:00Z",
        mode: "bot",
        error: REPAIR_MESSAGE,
        qr_payload: HOSTILE_QR_PAYLOAD,
      });
    await renderPage();

    await clickPair();

    await vi.waitFor(() => expect(poll).toHaveBeenCalled(), { timeout: 4000 });
    await vi.waitFor(() =>
      expect(container.textContent).toContain("hermes whatsapp"),
    );
    expectNoWhatsAppQrRendered();
    expect(toDataURL).not.toHaveBeenCalled();
  });

  it("ignores a qr_payload on an ordinary waiting response", async () => {
    // No onboarding status may drive a dashboard QR renderer — not even the
    // one the flow spends most of its time in.
    const toDataURL = qrMocks.toDataURL;
    vi.spyOn(api, "startWhatsAppOnboarding").mockResolvedValue({
      pairing_id: "legacy-waiting",
      status: "waiting",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      qr_payload: HOSTILE_QR_PAYLOAD,
    });
    vi.spyOn(api, "getWhatsAppOnboardingStatus").mockResolvedValue({
      pairing_id: "legacy-waiting",
      status: "waiting",
      expires_at: "2099-01-01T00:00:00Z",
      mode: "bot",
      qr_payload: HOSTILE_QR_PAYLOAD,
    });
    await renderPage();

    await clickPair();

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 1600));
    });
    expectNoWhatsAppQrRendered();
    expect(toDataURL).not.toHaveBeenCalled();
  });
});
