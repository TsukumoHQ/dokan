// flagship demo — step 1/4: intake
// Emits a deterministic batch of orders as its LAST stdout line. The flow engine passes a
// step's last stdout line to its dependents (here: `score` fans out over this array).
// No network, no randomness → the run is a pure function of its input (cacheable + provable).
function input() {
  try {
    let v = JSON.parse(process.env.DOKAN_INPUT || "{}");
    return typeof v === "string" ? JSON.parse(v) : v; // tolerate a double-encoded input
  } catch {
    return {};
  }
}

const inp = input();
const n = Number(inp.flow_input?.count ?? inp.count ?? 5) || 5;
const GEO = ["CH", "FR", "US", "NG", "DE"];
const orders = Array.from({ length: n }, (_, i) => ({
  id: 1000 + i,
  amount: 50 + i * 120, // varied, climbing
  country: GEO[i % GEO.length],
}));
console.log(JSON.stringify(orders)); // last stdout line = this step's output
