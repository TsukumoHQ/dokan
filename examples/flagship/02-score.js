// flagship demo — step 2/4: score  (runs once PER order — the `map` fan-out)
// The flow expands this step into one container run per element of `intake`'s array; each
// child reads its element from DOKAN_INPUT.step. Deterministic risk score (amount + geo).
function input() {
  try {
    let v = JSON.parse(process.env.DOKAN_INPUT || "{}");
    return typeof v === "string" ? JSON.parse(v) : v;
  } catch {
    return {};
  }
}

const order = input().step || {};
const geoRisk = { NG: 40, US: 10 }[order.country] ?? 5;
const score = Math.min(100, Math.round((order.amount || 0) / 10) + geoRisk);
console.log(String(score)); // last stdout line = this child's output
