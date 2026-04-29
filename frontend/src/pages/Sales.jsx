import Layout from "../components/Layout";
import axios from "axios";
import FileUpload from "../components/FileUpload";
import SalesDashboard from "../components/SalesDashboard";
import { useState } from "react";
import "../styles/sales.css";

const Sales = () => {
  const [file, setFile] = useState({ name: "sample_dataset.csv" }); // Simulate uploaded file
  const [uploading, setUploading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [outputPath, setOutputPath] = useState("sample_output_path"); // Simulate processing complete

  const handleFileSelect = async (selectedFile) => {
    setFile(selectedFile);
    setUploading(true);

    try {
      // Generate file key for s3
      const fileKey = `input/${selectedFile.name}`;

      // Get presigned URL from backend
      const res = await axios.get("http://localhost:8000/presigned-url", {
        params: { file_key: fileKey },
      });

      const presignedUrl = res.data.url;

      // Upload file to S3 using presigned URL
      await axios.put(presignedUrl, selectedFile, {
        headers: {
          "Content-Type": "application/octet-stream",
        },
      });

      console.log("File uploaded successfully");

      // Trigger graph execution
      setProcessing(true);

      const graphRes = await axios.post("http://localhost:8000/run-graph", {
        input_filename: selectedFile.name,
      });

      console.log("Graph executed successfully:", graphRes.data);
      setOutputPath(graphRes.data.output_path);
    } catch (err) {
      console.error("Error uploading file:", err);
    } finally {
      setUploading(false);
      setProcessing(false);
    }
  };

  const handleRemove = () => {
    setFile(null);
    setOutputPath(null);
  };

  return (
    <Layout>
      <h1>Sales</h1>

      {!file ? (
        <FileUpload onFileSelect={handleFileSelect} />
      ) : (
        <div className="sales-container">
          {/* ── Top Bar ── */}
          <div className="file-bar">
            <div className="file-info">
              <span className="file-label">Uploaded File</span>
              <span className="file-name">{file.name}</span>

              {uploading && (
                <span className="status uploading">Uploading...</span>
              )}
              {processing && (
                <span className="status processing">Processing...</span>
              )}
            </div>

            <button className="btn-remove" onClick={handleRemove}>
              Remove
            </button>
          </div>

          {/* ── Dashboard ── */}
          {outputPath && <SalesDashboard />}
        </div>
      )}
    </Layout>
  );
};

export default Sales;
