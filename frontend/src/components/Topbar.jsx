
const Topbar = ({ title }) => {
  return (
    <div className="topbar">
      <h1>{title}</h1>
      <div className="topbar-right">
        {/* Icons are gone! */}
        <div className="avatar-sm">JD</div>
        <span className="user-name">John Doe</span>
      </div>
    </div>
  );
};

export default Topbar;