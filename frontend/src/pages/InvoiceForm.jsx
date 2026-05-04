import { useState } from "react";

export default function InvoiceForm() {
  const [items, setItems] = useState([
    { id: "row-0", itemName: "", description: "", quantity: 1, unitPrice: 0, taxRate: 18 },
  ]);
  const [discount, setDiscount] = useState(0);
  const [loading, setLoading] = useState(false);

  // --- DERIVED TOTALS ---
  let subtotal = 0;
  let taxTotal = 0;
  items.forEach((item) => {
    const qty = parseFloat(item.quantity) || 0;
    const price = parseFloat(item.unitPrice) || 0;
    const tax = parseFloat(item.taxRate) || 0;
    const itemTotal = qty * price;
    subtotal += itemTotal;
    taxTotal += (itemTotal * tax) / 100;
  });
  const grandTotal = subtotal + taxTotal - (parseFloat(discount) || 0);

  const addRow = () => {
    setItems([
      ...items,
      { id: Date.now(), itemName: "", description: "", quantity: 1, unitPrice: 0, taxRate: 18 },
    ]);
  };

  const removeRow = (id) => {
    if (items.length > 1) setItems(items.filter((item) => item.id !== id));
  };

  const handleItemChange = (id, field, value) => {
    setItems(items.map((item) => (item.id === id ? { ...item, [field]: value } : item)));
  };

  // ✅ FIXED: Read form values via e.target.elements (by name), not e.target[index]
  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);

    const f = e.target.elements;

    const payload = {
      customer: {
        name: f.customerName.value,
        email: f.customerEmail.value,
        phone: f.customerPhone.value,
        gstin: f.gstin.value,
        address: f.address.value,
      },
      issueDate: f.issueDate.value,
      dueDate: f.dueDate.value,
      discount: parseFloat(discount) || 0,
      // eslint-disable-next-line no-unused-vars
      items: items.map(({ id: _id, ...rest }) => ({
        ...rest,
        quantity: parseFloat(rest.quantity),
        unitPrice: parseFloat(rest.unitPrice),
        taxRate: parseFloat(rest.taxRate),
      })),
    };

    try {
      const res = await fetch("http://localhost:8080/api/invoice/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errText = await res.text();
        console.error("Server error:", errText);
        alert("Failed to generate invoice. Check console for details.");
        return;
      }

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "invoice.pdf";
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Request failed:", err);
      alert("Network error. Is the backend running?");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="invoice-form-card">
      {/* Auto-generated badge */}
      <div className="auto-badge">
        <svg width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2.5" viewBox="0 0 24 24">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
        </svg>
        Invoice number is auto-generated
      </div>

      <form onSubmit={handleSubmit}>

        {/* ── Customer Details ── */}
        <div className="form-section-title">Customer Details</div>
        <div className="form-grid">
          <div className="field">
            <label>Customer Name</label>
            <input type="text" name="customerName" placeholder="Acme Corp" />
          </div>
          <div className="field">
            <label>Customer Email</label>
            <input type="email" name="customerEmail" placeholder="billing@acme.com" />
          </div>
          <div className="field">
            <label>Customer Phone</label>
            <input type="text" name="customerPhone" placeholder="+91 98765 43210" />
          </div>
          <div className="field">
            <label>GSTIN</label>
            <input type="text" name="gstin" placeholder="07ABCDE1234F1Z5" />
          </div>
          <div className="field span2">
            <label>Customer Address</label>
            <input type="text" name="address" placeholder="123, MG Road, New Delhi" />
          </div>
        </div>

        {/* ── Invoice Details ── */}
        <div className="form-section-title">Invoice Details</div>
        <div className="form-grid">
          <div className="field">
            <label>Issue Date</label>
            <input type="date" name="issueDate" />
          </div>
          <div className="field">
            <label>Due Date</label>
            <input type="date" name="dueDate" />
          </div>
          <div className="field">
            <label>Discount (Rs.)</label>
            <input
              type="number"
              name="discount"
              value={discount}
              onChange={(e) => setDiscount(e.target.value)}
              placeholder="0"
              min="0"
            />
          </div>
          <div className="field">
            <label>Notes</label>
            <textarea name="notes" placeholder="Payment due within 30 days" rows={1} />
          </div>
        </div>

        {/* ── Line Items ── */}
        <div className="form-section-title">Line Items</div>
        <div className="line-col-headers">
          <div className="line-col-header">Item Name</div>
          <div className="line-col-header">Description</div>
          <div className="line-col-header">Qty</div>
          <div className="line-col-header">Unit Price</div>
          <div className="line-col-header">Tax Rate %</div>
          <div />
        </div>

        <div id="rows">
          {items.map((item) => (
            <div className="item-row" key={item.id}>
              <input
                type="text"
                placeholder="e.g. Web design"
                value={item.itemName}
                onChange={(e) => handleItemChange(item.id, "itemName", e.target.value)}
              />
              <input
                type="text"
                placeholder="Optional"
                value={item.description}
                onChange={(e) => handleItemChange(item.id, "description", e.target.value)}
              />
              <input
                type="number"
                value={item.quantity}
                onChange={(e) => handleItemChange(item.id, "quantity", e.target.value)}
                min="0"
              />
              <input
                type="number"
                placeholder="0.00"
                value={item.unitPrice}
                onChange={(e) => handleItemChange(item.id, "unitPrice", e.target.value)}
                min="0"
              />
              <input
                type="number"
                value={item.taxRate}
                onChange={(e) => handleItemChange(item.id, "taxRate", e.target.value)}
                min="0"
                max="100"
              />
              <button type="button" className="btn-remove" onClick={() => removeRow(item.id)}>
                ✕
              </button>
            </div>
          ))}
        </div>

        <button type="button" className="btn-add-item" onClick={addRow}>
          + Add Item
        </button>

        {/* ── Totals ── */}
        <div className="totals-box">
          <div className="totals-row">
            <span className="label">Subtotal</span>
            <span className="value">Rs. {subtotal.toFixed(2)}</span>
          </div>
          <div className="totals-row">
            <span className="label">Tax</span>
            <span className="value">Rs. {taxTotal.toFixed(2)}</span>
          </div>
          <div className="totals-row">
            <span className="label">Discount</span>
            <span className="value">Rs. {parseFloat(discount || 0).toFixed(2)}</span>
          </div>
          <div className="totals-row grand">
            <span className="label">Total</span>
            <span className="value">Rs. {grandTotal.toFixed(2)}</span>
          </div>
        </div>

        {/* ── Submit ── */}
        <button type="submit" className="btn-generate" disabled={loading}>
          {loading ? "Generating..." : "Generate Invoice PDF"}
          <svg width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24">
            <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
            <path d="M14 2v6h6" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
            <polyline points="10,9 9,9 8,9" />
          </svg>
        </button>

      </form>
    </div>
  );
}