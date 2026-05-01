import axios from "axios";
import FileUpload from "./FileUpload";
import { useState } from "react";

const Sales = () => {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [outputPath, setOutputPath] = useState(null);

  const handleFileSelect = async (selectedFile) => {
    setFile(selectedFile);
    setUploading(true);

    try {
      const fileKey = `input/${selectedFile.name}`;
      const res = await axios.get("http://localhost:8000/presigned-url", {
        params: { file_key: fileKey },
      });

      const presignedUrl = res.data.url;

      await axios.put(presignedUrl, selectedFile, {
        headers: { "Content-Type": "application/octet-stream" },
      });

      setProcessing(true);
      const graphRes = await axios.post("http://localhost:8000/run-graph", {
        input_filename: selectedFile.name,
      });

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
    <>
    

      <div className="invoice-form-card">
        {!file ? (
          <FileUpload onFileSelect={handleFileSelect} />
        ) : (
          <div style={{ marginTop: "20px" }}>
            <h3>Uploaded File:</h3>
            <p>{file.name}</p>

            {uploading && <p className="status-text">Uploading...</p>}
            {processing && <p className="status-text">Processing...</p>}

            {outputPath && (
              <div className="output-box" style={{ marginTop: "10px" }}>
                <strong>Output Generated:</strong>
                <p>{outputPath}</p>
              </div>
            )}

            <button 
              className="btn-remove" 
              style={{ marginTop: "20px" }} 
              onClick={handleRemove}
            >
              Remove and Upload New
            </button>
          </div>
        )}
      </div>
    </>
  );
};

export default Sales;