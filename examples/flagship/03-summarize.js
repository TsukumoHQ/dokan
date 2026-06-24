// flagship demo — step 3/4: summarize
// Depends on BOTH intake (the orders) and score (the map parent's aggregated child outputs,
// a JSON array of the per-order scores). Emits a structured result for the operator/agent
// AND a plain branch token as its last stdout line for the next step's `when` gate.
function input() {
  try {
    let v = JSON.parse(process.env.DOKAN_INPUT || "{}");
    return typeof v === "string" ? JSON.parse(v) : v;
  } catch {
    return {};
  }
}

const inp = input();
const orders = JSON.parse(inp.deps?.intake || "[]");
const scores = JSON.parse(inp.deps?.score || "[]").map(Number);
const THRESHOLD = 70;
const flagged = scores.filter((s) => s >= THRESHOLD).length;
const maxScore = scores.length ? Math.max(...scores) : 0;

// Structured result: captured off stdout (not logged), returned by get_flow_run / the receipt.
console.log(
  `::dokan:result:: ${JSON.stringify({ orders: orders.length, flagged, max_score: maxScore, threshold: THRESHOLD })}`,
);
// Last PLAIN stdout line = the branch token the `alert` step's `when` gates on.
console.log(flagged > 0 ? "FLAGGED" : "CLEAN");
