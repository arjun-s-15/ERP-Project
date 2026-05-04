import Sidebar from './Sidebar';

const Layout = ({ children, hideSidebar = false }) => {
  if (hideSidebar) return <>{children}</>;

  return (
    <div className="with-sidebar">
      <Sidebar />
      <main className="main">
        {children}
      </main>
    </div>
  );
};

export default Layout;