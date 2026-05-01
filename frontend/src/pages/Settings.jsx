
const Settings = () => {
  return (
    <>
      {/* <div className="topbar">
        <h1>Settings</h1>
        <div className="topbar-right">
          <div className="avatar-sm">JD</div>
          <span className="user-name">John Doe</span>
        </div>
      </div> */}

      <div className="content-area">
        <div className="card">
          <div className="card-header">
            <div>
              <div className="card-title">User Profile</div>
              <div className="card-subtitle">Update your personal information and photo</div>
            </div>
            <span className="role-badge">
              <svg width="10" height="10" fill="currentColor" viewBox="0 0 24 24">
                <path d="M12 2a5 5 0 100 10A5 5 0 0012 2zm0 12c-5.33 0-8 2.67-8 4v2h16v-2c0-1.33-2.67-4-8-4z"/>
              </svg>
              Admin
            </span>
          </div>

          <div className="avatar-section">
            <div className="avatar-lg">JD</div>
            <div className="avatar-info">
              <h3>John Doe</h3>
              <p>john.doe@company.com</p>
              <button className="btn-upload-photo">
                <svg width="13" height="13" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24">
                  <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                  <polyline points="17,8 12,3 7,8"/>
                  <line x1="12" y1="3" x2="12" y2="15"/>
                </svg>
                Upload Photo
              </button>
            </div>
          </div>

          <div className="form-grid">
            <div className="field"><label>First Name</label><input type="text" defaultValue="John" /></div>
            <div className="field"><label>Last Name</label><input type="text" defaultValue="Doe" /></div>
            <div className="field span2"><label>Email Address</label><input type="email" defaultValue="john.doe@company.com" /></div>
            <div className="field"><label>Phone Number</label><input type="tel" defaultValue="+1 (555) 000-0000" /></div>
            <div className="field"><label>Job Title</label><input type="text" defaultValue="ERP Administrator" /></div>
            <div className="field">
              <label>Department</label>
              <select defaultValue="Finance">
                <option>Finance</option>
                <option>Sales</option>
                <option>IT</option>
              </select>
            </div>
            <div className="field"><label>Location</label><input type="text" defaultValue="New York, USA" /></div>
            <div className="field span2">
              <label>Bio <span className="label-hint">(optional)</span></label>
              <textarea defaultValue="Manages ERP operations and system configurations across the organization." />
            </div>
          </div>

          <div className="form-footer">
            <button className="btn-cancel">Discard</button>
            <button className="btn-save">
              <svg width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2.5" viewBox="0 0 24 24">
                <polyline points="20,6 9,17 4,12"/>
              </svg>
              Save Changes
            </button>
          </div>
        </div>
      </div>
    </>
  );
};

export default Settings;