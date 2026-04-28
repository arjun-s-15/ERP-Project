import Sidebar from "./Sidebar";
import React from "react";

const Layout = ({ children }) => {
  return (
    <>
      <Sidebar />
      <main className="main">{children}</main>
    </>
  );
};

export default Layout;
