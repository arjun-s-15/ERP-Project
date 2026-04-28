import React, { useRef } from "react";
import "../styles/upload.css";

const FileUpload = ({ onFileSelect }) => {
  const fileInputRef = useRef(null);

  const handleClick = (e) => {
    e.preventDefault();
    fileInputRef.current.click();
  };

  const handleChange = (e) => {
    const file = e.target.files[0];
    if (file && onFileSelect) {
      onFileSelect(file);
    }
  };

  return (
    <div className="upload-wrapper">
      <label className="upload-zone">
        <svg
          className="upload-icon"
          width="56"
          height="56"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          viewBox="0 0 24 24"
        >
          <path d="M12 16V4m0 0L8 8m4-4l4 4" />
          <path d="M20 16.5A3.5 3.5 0 0116.5 20H7.5A3.5 3.5 0 014 16.5v-1" />
        </svg>

        <div className="upload-text">
          Drag & drop your file here or{" "}
          <span className="browse-link" onClick={handleClick}>
            Browse
          </span>
        </div>

        <button className="btn-upload" onClick={handleClick}>
          Upload
        </button>

        <div className="upload-hint">Supported Formats: CSV, XLSX, PARQUET</div>
      </label>

      <input
        type="file"
        ref={fileInputRef}
        accept=".csv,.xlsx,.parquet"
        style={{ display: "none" }}
        onChange={handleChange}
      />
    </div>
  );
};

export default FileUpload;
