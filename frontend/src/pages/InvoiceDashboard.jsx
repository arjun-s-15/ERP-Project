import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";

const InvoiceDashboard = () => {
  const navigate = useNavigate();

  const [invoices, setInvoices] = useState([]);
  const [filtered, setFiltered] = useState([]);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);

  // Helper to ensure all requests include the session cookie
  const authenticatedFetch = (url, options = {}) => {
    return fetch(url, {
      ...options,
      credentials: 'include', // 🔥 Critical for Spring Security session persistence[cite: 2]
    });
  };

  const fetchInvoices = useCallback(async (isMounted = true) => {
    try {
      const res = await authenticatedFetch("http://localhost:8080/api/invoice");
      if (!res.ok) throw new Error("Failed to fetch");
      const data = await res.json();
      if (isMounted) {
        setInvoices(data);
        setFiltered(data);
      }
    } catch (err) {
      if (isMounted) console.error("Failed to fetch invoices:", err);
    } finally {
      if (isMounted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    let isMounted = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    fetchInvoices(isMounted);
    return () => { isMounted = false; };
  }, [fetchInvoices]);

  const handleSearch = (e) => {
    const q = e.target.value.toLowerCase();
    setSearch(q);
    setFiltered(
      invoices.filter(
        (inv) =>
          inv.invoiceNumber?.toLowerCase().includes(q) ||
          inv.customer?.toLowerCase().includes(q) ||
          inv.status?.toLowerCase().includes(q)
      )
    );
  };

  const updateStatus = async (id, status) => {
    try {
      const res = await authenticatedFetch(
        `http://localhost:8080/api/invoice/${id}/status?status=${status}`,
        { method: "PATCH" }
      );
      if (!res.ok) throw new Error("Failed to update status");
      await fetchInvoices();
    } catch (err) {
      console.error("Status update failed:", err);
    }
  };

  const deleteInvoice = async (id) => {
    if (!window.confirm("Delete this invoice?")) return;
    try {
      const res = await authenticatedFetch(`http://localhost:8080/api/invoice/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete");
      await fetchInvoices();
    } catch (err) {
      console.error("Delete failed:", err);
    }
  };

  const getBadgeClass = (status) => {
    const classes = {
      PAID: "badge-paid",
      SENT: "badge-pending",
      OVERDUE: "badge-hold",
      DRAFT: "badge-new",
    };
    return `badge ${classes[status] || "badge-new"}`;
  };

  const canSend = (status) => status === "DRAFT";
  const canMarkPaid = (status) => status === "SENT" || status === "OVERDUE";

  return (
    <>
      <div className="page-header">
        <div>
          <h1>Invoice Dashboard</h1>
          <p className="card-subtitle">Monitor billing and invoices</p>
        </div>

        <div className="btn-group" style={{ alignItems: "center" }}>
          <input
            type="text"
            placeholder="Search invoices..."
            value={search}
            onChange={handleSearch}
            style={{
              width: "220px",
              padding: "8px 12px",
              borderRadius: "6px",
              border: "1px solid var(--border)",
            }}
          />
          <button className="btn-create" onClick={() => navigate("/invoice/new")}>
            + Create Invoice
          </button>
        </div>
      </div>

      <div className="card">
        <div className="table-header">
          <h3 className="card-title">Recent Invoices</h3>
        </div>

        {loading ? (
          <p style={{ padding: "20px" }}>Loading invoices...</p>
        ) : filtered.length === 0 ? (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <p>{search ? "No invoices match your search." : "No invoices yet"}</p>
            {!search && (
              <button
                className="btn-create"
                onClick={() => navigate("/invoice/new")}
                style={{ marginTop: "12px" }}
              >
                Create your first invoice
              </button>
            )}
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Invoice</th>
                <th>Customer</th>
                <th>Date</th>
                <th>Amount</th>
                <th>Status</th>
                <th style={{ textAlign: "right" }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((inv) => (
                <tr key={inv.id}>
                  <td>{inv.invoiceNumber}</td>
                  <td>{inv.customer}</td>
                  <td>{inv.issueDate}</td>
                  <td><strong>₹{inv.totalAmount}</strong></td>
                  <td>
                    <span className={getBadgeClass(inv.status)}>{inv.status}</span>
                  </td>
                  <td className="actions-cell">
                    <button
                      className="action-btn"
                      onClick={async () => {
                        try {
                          const res = await authenticatedFetch(`http://localhost:8080/api/invoice/${inv.id}/url`);
                          const data = await res.json();
                          window.open(data.url, "_blank");
                        } catch (err) {
                          console.error("Failed to get invoice URL:", err);
                          alert("Invoice URL expired.");
                        }
                      }}
                    >
                      View
                    </button>
                    {canSend(inv.status) && (
                      <button
                        className="action-btn"
                        onClick={() => updateStatus(inv.id, "SENT")}
                      >
                        Send
                      </button>
                    )}
                    {canMarkPaid(inv.status) && (
                      <button
                        className="action-btn"
                        onClick={() => updateStatus(inv.id, "PAID")}
                      >
                        Mark Paid
                      </button>
                    )}
                    <button
                      className="action-btn delete-text"
                      onClick={() => deleteInvoice(inv.id)}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
};

export default InvoiceDashboard;