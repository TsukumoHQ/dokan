// flagship demo — step 4/4: alert  (the `when` branch)
// Runs ONLY when `summarize` emitted "FLAGGED" — the flow gates this step on
// when: { ref: "deps.summarize", op: "eq", value: "FLAGGED" }. On a CLEAN batch it is
// skipped (and the skip would propagate to anything depending on it).
console.log(
  "::dokan:result:: " +
    JSON.stringify({ alerted: true, action: "routed high-risk orders to manual review" }),
);
