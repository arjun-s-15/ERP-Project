import { BrowserRouter as Router, Routes, Route, Navigate, useLocation } from 'react-router-dom';

// Global Styles
import './styles/erp.css';

// Components
import Sidebar from './components/Sidebar';
import Topbar from './components/Topbar'; // Import your new Topbar

// Pages
import Login from './pages/Login';
import Sales from "./pages/Sales";
import InvoiceForm from './pages/InvoiceForm';
import Settings from './pages/Settings';

/**
 * Layout wrapper that automatically adds Sidebar and Topbar.
 */
const MainLayout = ({ children }) => {
  const location = useLocation();

  // Map the URL paths to friendly titles for the Topbar
  const getTitle = (path) => {
    if (path.startsWith('/invoice')) return 'New Invoice';
    if (path === '/sales') return 'Sales';
    if (path === '/settings') return 'Settings';
    return 'ERP Dashboard';
  };

  return (
    <div className="with-sidebar">
      <Sidebar />
      <main className="main">
        {/* Topbar is now global for all protected routes */}
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
        {/* Public Routes - No Layout, No Topbar */}
        <Route path="/login" element={<Login />} />
        <Route path="/" element={<Navigate to="/login" replace />} />

        {/* Protected Routes - Wrapped in MainLayout */}
        <Route path="/sales" element={<MainLayout><Sales /></MainLayout>} />
        <Route path="/invoice/new" element={<MainLayout><InvoiceForm /></MainLayout>} />
        <Route path="/settings" element={<MainLayout><Settings /></MainLayout>} />


      </Routes>
    </Router>
  );
}

export default App;