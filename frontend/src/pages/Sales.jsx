import Layout from "../components/Layout";
import axios from "axios";
import FileUpload from "../components/FileUpload";
import { useState } from "react";

const Sales = () => {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);

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
    } catch (err) {
      console.error("Error uploading file:", err);
    } finally {
      setUploading(false);
    }
  };

  const handleRemove = () => {
    setFile(null);
  };

  return (
    <Layout>
      <h1>Sales</h1>

      {!file ? (
        <FileUpload onFileSelect={handleFileSelect} />
      ) : (
        <div style={{ marginTop: "20px" }}>
          <h3>Uploaded File:</h3>
          <p>{file.name}</p>

          {uploading && <p>Uploading...</p>}
          <button onClick={handleRemove}>Remove File</button>
        </div>
      )}
    </Layout>
  );
};

export default Sales;
