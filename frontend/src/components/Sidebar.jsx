import { NavLink } from "react-router-dom";

const navLinks = [

  {
    path: "/invoices",
    label: "Invoices",
    icon: (
      <>
        <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
        <polyline points="14,2 14,8 20,8" />
      </>
    ),
  },
  {
    path: "/inventory", // ── New Inventory Link ──
    label: "Inventory",
    icon: (
      <>
        <path d="M21 8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16Z" />
        <path d="m3.3 7 8.7 5 8.7-5" />
        <path d="M12 22V12" />
      </>
    ),
  },

  {
    path: "/sales",
    label: "Sales",
    icon: <path d="M3 3h18v4H3zM3 10h18v4H3zM3 17h18v4H3z" />,
  },

  {
    path: "/settings",
    label: "Settings",
    icon: (
      <>
        <circle cx="12" cy="12" r="3" />
        <path d="M19.4 15a1.65 1.65 0 00.33 1.82..." />
      </>
    ),
  },
];

const Sidebar = () => {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">Insighto</div>

      <nav>
        {navLinks.map((link) => (
          <NavLink
            key={link.path}
            to={link.path}
            className={({ isActive }) =>
              `nav-item ${isActive ? "active" : ""}`
            }
          >
            <svg
              className="nav-icon"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              viewBox="0 0 24 24"
            >
              {link.icon}
            </svg>

            {link.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
};

export default Sidebar;