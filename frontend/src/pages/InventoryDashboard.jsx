import { useState, useCallback } from "react";

// Pointing to your Inventory Microservice
const API_BASE = "http://localhost:8082/inventory";

const InventoryPage = () => {
  // ── State ────────────────────────────────────────────────────────────────
  const [stock, setStock] = useState(null);
  const [log, setLog] = useState([]);
  const [loading, setLoading] = useState(false);
  const [feedback, setFeedback] = useState({ msg: "", isError: false });

  // Lookup fields (for the search bar)
  const [lookup, setLookup] = useState({ pid: "", wid: "" });

  // Action fields (for the form)
  const [action, setAction] = useState("reserve");
  const [form, setForm] = useState({ pid: "", wid: "", qty: "", ref: "" });

  // ── Helpers ──────────────────────────────────────────────────────────────
  
  // Reusing your Spring Security session persistence pattern
  const authenticatedFetch = (url, options = {}) =>
  fetch(url, {
    ...options,
    credentials: "omit"
  });
  const flash = (msg, isError = false) => {
    setFeedback({ msg, isError });
    setTimeout(() => setFeedback({ msg: "", isError: false }), 4000);
  };

  const pushLog = useCallback((type, msg) =>
    setLog(prev => [
      { id: Date.now(), type, msg, time: new Date().toLocaleTimeString() },
      ...prev,
    ].slice(0, 20)),
  []);

  // ── API Calls ────────────────────────────────────────────────────────────

  const fetchStock = async () => {
    if (!lookup.pid || !lookup.wid) {
      flash("Enter Product and Warehouse IDs", true);
      return;
    }
    setLoading(true);
    try {
      const res = await authenticatedFetch(
        `${API_BASE}?productId=${lookup.pid}&warehouseId=${lookup.wid}`
      );
      if (!res.ok) throw new Error("Stock record not found");
      const data = await res.json();
      setStock(data);
      pushLog("FETCH", `P:${lookup.pid} W:${lookup.wid} synced`);
    } catch (e) {
      flash(e.message, true);
      setStock(null);
    } finally {
      setLoading(false);
    }
  };

  const submitAction = async () => {
    const { pid, wid, qty, ref } = form;
    if (!pid || !wid || !qty) return flash("Missing required fields", true);
    if (action !== "add" && !ref) return flash("Reference ID required for " + action, true);

    setLoading(true);
    try {
      const res = await authenticatedFetch(`${API_BASE}/${action}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          productId: Number(pid),
          warehouseId: Number(wid),
          quantity: Number(qty),
          ...(ref && { referenceId: ref }),
        }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.message || "Action failed");
      }

      const data = await res.json();
      flash(`${action.toUpperCase()} processed: Txn #${data.transactionId}`);
      pushLog(action.toUpperCase(), `Qty: ${qty} for P:${pid} (Txn: ${data.transactionId})`);

      // If the action was for the item currently being viewed, update the UI
      if (pid === lookup.pid && wid === lookup.wid) {
        setStock(prev => ({ ...prev, ...data }));
      }
      
      // Reset action-specific fields
      setForm(prev => ({ ...prev, qty: "", ref: "" }));
    } catch (e) {
      flash(e.message, true);
      pushLog("ERROR", e.message);
    } finally {
      setLoading(false);
    }
  };

  // ── UI Mappings ──────────────────────────────────────────────────────────

  const getBadgeClass = (type) => {
    const map = {
      ADD: "badge-paid",      // Green
      RESERVE: "badge-pending", // Orange/Yellow
      RELEASE: "badge-new",     // Blue
      DEDUCT: "badge-hold",     // Red
      FETCH: "badge-new",
      ERROR: "badge-hold",
    };
    return `badge ${map[type] || "badge-new"}`;
  };

  const ACTION_DESC = {
    add: "Inventory In: Increases available stock.",
    reserve: "Allocation: Moves available to reserved status.",
    release: "De-allocation: Moves reserved back to available.",
    deduct: "Fulfillment: Finalizes sale, removes from reserved.",
  };

  return (
    <>
      <div className="page-header">
        <div>
          <h1>Inventory Control</h1>
          <p className="card-subtitle">Manage warehouse stock and reservations</p>
        </div>
      </div>

      {/* Unified Feedback Toast */}
      {feedback.msg && (
        <div className={`alert ${feedback.isError ? 'alert-error' : 'alert-success'}`} style={{
          padding: "12px 16px", borderRadius: "8px", marginBottom: "20px", fontSize: "14px",
          backgroundColor: feedback.isError ? "#fee2e2" : "#dcfce7",
          color: feedback.isError ? "#991b1b" : "#166534",
          border: `1px solid ${feedback.isError ? "#fca5a5" : "#86efac"}`
        }}>
          {feedback.msg}
        </div>
      )}

      {/* ── Section: Stock Lookup ── */}
      <div className="card" style={{ marginBottom: "24px" }}>
        <div className="table-header">
          <h3 className="card-title">Live Stock Status</h3>
          <div className="btn-group">
             <input
              type="number" placeholder="Product ID"
              value={lookup.pid} onChange={e => setLookup({...lookup, pid: e.target.value})}
              style={inputStyle}
            />
            <input
              type="number" placeholder="Warehouse ID"
              value={lookup.wid} onChange={e => setLookup({...lookup, wid: e.target.value})}
              style={inputStyle}
            />
            <button className="btn-create" onClick={fetchStock} disabled={loading}>
              {loading ? "..." : "Fetch"}
            </button>
          </div>
        </div>

        {stock ? (
          <table>
            <thead>
              <tr>
                <th>Product ID</th>
                <th>Warehouse</th>
                <th>Available</th>
                <th>Reserved</th>
                <th>Last Updated</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{stock.productId || lookup.pid}</td>
                <td>{stock.warehouseId || lookup.wid}</td>
                <td><strong style={{color: "var(--primary)"}}>{stock.availableQuantity}</strong></td>
                <td>{stock.reservedQuantity}</td>
                <td>{stock.updatedAt ? new Date(stock.updatedAt).toLocaleString() : "—"}</td>
              </tr>
            </tbody>
          </table>
        ) : (
          <div style={{ padding: "40px", textAlign: "center", color: "var(--text-secondary)" }}>
            Perform a lookup to view inventory levels.
          </div>
        )}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "24px" }}>
        
        {/* ── Section: Actions ── */}
        <div className="card">
          <div className="table-header">
            <h3 className="card-title">Inventory Operations</h3>
          </div>
          <div style={{ padding: "20px" }}>
           <div className="btn-group" style={{ marginBottom: "20px", display: "flex", gap: "8px" }}>
  {["add", "reserve", "release", "deduct"].map(a => {
    const isActive = action === a;
    return (
      <button
        key={a}
        type="button"
        onClick={() => setAction(a)}
        style={{
          flex: 1,
          padding: "10px 5px",
          borderRadius: "6px",
          border: isActive ? "1px solid #2563eb" : "1px solid var(--border)",
          cursor: "pointer",
          fontSize: "12px",
          fontWeight: "bold",
          // 🔥 Selection Logic
          backgroundColor: isActive ? "#2563eb" : "#ffffff", // Blue when active
          color: isActive ? "#ffffff" : "#4b5563",           // White text when active
          boxShadow: isActive ? "0 2px 4px rgba(37, 99, 235, 0.2)" : "none",
          transition: "all 0.15s ease-in-out"
        }}
      >
        {a.toUpperCase()}
      </button>
    );
  })}
</div>

            <p style={{ fontSize: "13px", color: "#666", marginBottom: "20px", fontStyle: "italic" }}>
              {ACTION_DESC[action]}
            </p>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
              <div className="input-field">
                <label style={labelStyle}>Product ID</label>
                <input type="number" value={form.pid} onChange={e => setForm({...form, pid: e.target.value})} style={fullInputStyle} />
              </div>
              <div className="input-field">
                <label style={labelStyle}>Warehouse ID</label>
                <input type="number" value={form.wid} onChange={e => setForm({...form, wid: e.target.value})} style={fullInputStyle} />
              </div>
              <div className="input-field">
                <label style={labelStyle}>Quantity</label>
                <input type="number" value={form.qty} onChange={e => setForm({...form, qty: e.target.value})} style={fullInputStyle} />
              </div>
              <div className="input-field">
                <label style={labelStyle}>Reference ID</label>
                <input type="text" placeholder="e.g. ORDER-123" value={form.ref} onChange={e => setForm({...form, ref: e.target.value})} style={fullInputStyle} />
              </div>
            </div>

            <button className="btn-create" onClick={submitAction} style={{ width: "100%", marginTop: "20px" }} disabled={loading}>
              Execute {action.toUpperCase()}
            </button>
          </div>
        </div>

        {/* ── Section: Log ── */}
        <div className="card">
          <div className="table-header">
            <h3 className="card-title">Session Activity</h3>
            <button onClick={() => setLog([])} style={{ background: "none", border: "none", fontSize: "12px", color: "var(--text-secondary)", cursor: "pointer" }}>Clear</button>
          </div>
          <div style={{ maxHeight: "380px", overflowY: "auto" }}>
            {log.length === 0 ? (
              <p style={{ padding: "20px", textAlign: "center", color: "#999" }}>No activity recorded yet.</p>
            ) : (
              <table>
                <tbody>
                  {log.map(item => (
                    <tr key={item.id}>
                      <td style={{ width: "80px" }}><span className={getBadgeClass(item.type)}>{item.type}</span></td>
                      <td style={{ fontSize: "13px" }}>{item.msg}</td>
                      <td style={{ textAlign: "right", fontSize: "11px", color: "#999" }}>{item.time}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </>
  );
};

// Inline Styles to supplement your global CSS
const inputStyle = { padding: "8px 12px", borderRadius: "6px", border: "1px solid var(--border)", width: "120px" };
const fullInputStyle = { padding: "8px 12px", borderRadius: "6px", border: "1px solid var(--border)", width: "100%", boxSizing: "border-box" };
const labelStyle = { display: "block", fontSize: "11px", fontWeight: "bold", marginBottom: "4px", textTransform: "uppercase", color: "var(--text-secondary)" };

export default InventoryPage;