import { BrowserRouter as Router, Routes, Route, Navigate, useLocation } from 'react-router-dom';

// Global Styles
import './styles/erp.css';

// Components
import Sidebar from './components/Sidebar';
import Topbar from './components/Topbar';

// Pages
import Login from './pages/Login';
import Sales from "./pages/Sales";
import InvoiceForm from './pages/InvoiceForm';
import Settings from './pages/Settings';
import InvoiceDashboard from './pages/InvoiceDashboard';

const MainLayout = ({ children }) => {
  const location = useLocation();

  // ✅ FIXED: exact path matching so /invoice/new doesn't bleed into /invoices
  const getTitle = (path) => {
    if (path === '/invoice/new') return 'New Invoice';
    if (path === '/invoices')    return 'Invoices';
    if (path === '/sales')       return 'Sales';
    if (path === '/settings')    return 'Settings';
    return 'ERP Dashboard';
  };

  return (
    <div className="with-sidebar">
      <Sidebar />
      <main className="main">
        <Topbar title={getTitle(location.pathname)} />
        <div className="page-content">
          {children}
        </div>
      </main>
    </div>
  );
};

function App() {
  return (
    <Router>
      <Routes>
        {/* Public */}
        <Route path="/login" element={<Login />} />
        <Route path="/" element={<Navigate to="/login" replace />} />

        {/* Protected */}
        <Route path="/sales"       element={<MainLayout><Sales /></MainLayout>} />
        <Route path="/invoice/new" element={<MainLayout><InvoiceForm /></MainLayout>} />
        <Route path="/invoices"    element={<MainLayout><InvoiceDashboard /></MainLayout>} />
        <Route path="/settings"    element={<MainLayout><Settings /></MainLayout>} />
      </Routes>
    </Router>
  );
}

export default App;