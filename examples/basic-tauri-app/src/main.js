const { invoke } = window.__TAURI__.core;

const policyEl = document.querySelector("#policy");
const errorEl = document.querySelector("#error");
const applyButton = document.querySelector("#apply-update");
const downloadButton = document.querySelector("#download-update");

function describePolicy(policy) {
  if (!policy) return "Waiting for first check-in…";
  switch (policy.type) {
    case "current":
      return `Current (environment: ${policy.environment}, issued ${policy.issuedAt})`;
    case "held":
      return `Held${policy.reason ? ` — ${policy.reason}` : ""} (environment: ${policy.environment})`;
    case "updateEligible":
      return `Update eligible${policy.priority ? ` (${policy.priority} priority)` : ""} (environment: ${policy.environment})`;
    default:
      return "Unknown policy";
  }
}

async function refresh() {
  const status = await invoke("barrel_get_status");
  policyEl.textContent = describePolicy(status.policy);
  errorEl.textContent = status.lastUpdateError
    ? `${status.lastUpdateError.title} (${status.lastUpdateError.failureMode})`
    : "None this session";
}

applyButton.addEventListener("click", () => {
  void invoke("barrel_apply_update");
});

downloadButton.addEventListener("click", () => {
  void invoke("barrel_download_update");
});

void refresh();
setInterval(refresh, 2000);
