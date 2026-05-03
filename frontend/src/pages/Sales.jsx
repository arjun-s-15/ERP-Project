import axios from "axios";
import FileUpload from "../components/FileUpload";
import SalesDashboard from "../components/SalesDashboard";
import { useState, useEffect } from "react";
import "../styles/sales.css";

const Sales = () => {
  const [file, setFile] = useState(() => {
    const saved = localStorage.getItem("sales_file");
    return saved ? JSON.parse(saved) : null;
  });
  const [uploading, setUploading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [outputPath, setOutputPath] = useState(() => {
    return localStorage.getItem("sales_output_path") || null;
  });
  const [steps, setSteps] = useState([]);

  useEffect(() => {
    if (file) localStorage.setItem("sales_file", JSON.stringify(file));
    else localStorage.removeItem("sales_file");
  }, [file]);

  useEffect(() => {
    if (outputPath) localStorage.setItem("sales_output_path", outputPath);
    else localStorage.removeItem("sales_output_path");
  }, [outputPath]);

  useEffect(() => {
    if (steps.length > 0 && steps.every((s) => s.status === "done")) {
      setSteps([]);
    }
  }, [steps]);

  const setStep = (label, status) => {
    setSteps((prev) => {
      const existing = prev.findIndex((s) => s.label === label);
      if (existing >= 0) {
        const updated = [...prev];
        updated[existing] = { label, status };
        return updated;
      }
      return [...prev, { label, status }];
    });
  };

  const pollDag = async (dagRunId) => {
    const maxAttempts = 60;
    const interval = 20000; // 10 seconds

    for (let i = 0; i < maxAttempts; i++) {
      const res = await axios.post(
        "http://localhost:8001/pipelines/dag-status",
        {
          dag_id: "master_sales_pipeline",
          dag_run_id: dagRunId,
        },
      );

      const state = res.data.state;
      if (state === "success") return;
      if (state === "failed") throw new Error("Airflow DAG failed");

      await new Promise((resolve) => setTimeout(resolve, interval));
    }
    throw new Error("Airflow DAG polling timed out");
  };

  const handleFileSelect = async (selectedFile) => {
    setFile(selectedFile);
    setUploading(true);
    setSteps([]);

    try {
      const fileKey = `input/${selectedFile.name}`;

      setStep("Getting upload URL", "loading");
      const res = await axios.get("http://localhost:8000/presigned-url", {
        params: { file_key: fileKey },
      });
      setStep("Getting upload URL", "done");

      setStep("Uploading file to S3", "loading");
      await axios.put(res.data.url, selectedFile, {
        headers: { "Content-Type": "application/octet-stream" },
      });
      setStep("Uploading file to S3", "done");

      setProcessing(true);

      setStep("Running multi-agent transformation", "loading");
      // const graphRes = await axios.post("http://localhost:8000/run-graph", {
      //   input_filename: selectedFile.name,
      // });
      // setOutputPath(graphRes.data.s3_output_path);
      // console.log("graphRes.data:", graphRes.data);
      setOutputPath(`data/${selectedFile.name}`); // For testing without multi-agent graph
      setStep("Running multi-agent transformation", "done");

      // const outputFilename = graphRes.data.s3_output_path.split("/").pop();
      const outputFilename = `transformed_sample_dataset_6m.parquet`; // For testing without multi-agent graph

      setStep("Triggering Airflow pipeline", "loading");
      const airflowRes = await axios.post(
        "http://localhost:8001/pipelines/trigger-master",
        {
          file_key: `data/${outputFilename}`,
        },
      );
      setStep("Triggering Airflow pipeline", "done");

      const dagRunId = airflowRes.data.airflow_response.dag_run_id;

      setStep("Pipeline running in background", "loading");
      await pollDag(dagRunId);
      setStep("Pipeline running in background", "done");

      console.log("Master pipeline triggered:", airflowRes.data);
    } catch (err) {
      setSteps((prev) =>
        prev.map((s) =>
          s.status === "loading" ? { ...s, status: "error" } : s,
        ),
      );
      console.error("Error:", err);
    } finally {
      setUploading(false);
      setProcessing(false);
    }
  };

  const handleRemove = () => {
    setFile(null);
    setOutputPath(null);
    setSteps([]);
    localStorage.removeItem("sales_file");
    localStorage.removeItem("sales_output_path");
    // clear dashboard cache too
    [
      "totalSales",
      "monthlySales",
      "dowSales",
      "dailyTotal",
      "storeDaily",
      "storeForecast",
      "totalForecast",
    ].forEach((key) => localStorage.removeItem(`dashboard_${key}`));
  };

  const STATUS_ICON = {
    loading: <span className="step-spinner" />,
    running: <span className="step-spinner" />,
    done: <span className="step-icon done">✓</span>,
    error: <span className="step-icon error">✕</span>,
  };

  return (
    <>
      {!file ? (
        <FileUpload onFileSelect={handleFileSelect} />
      ) : (
        <div className="sales-container">
          <div className="file-bar">
            <div className="file-info">
              <span className="file-label">Uploaded File</span>
              <span className="file-name">{file.name}</span>
            </div>
            <button className="btn-remove" onClick={handleRemove}>
              Remove
            </button>
          </div>

          {steps.length > 0 && (
            <div className="steps-card">
              {steps.map((step) => (
                <div key={step.label} className={`step-row ${step.status}`}>
                  {STATUS_ICON[step.status]}
                  <span className="step-label">{step.label}</span>
                </div>
              ))}
            </div>
          )}

          {outputPath && steps.every((s) => s.status === "done") && (
            <SalesDashboard />
          )}
        </div>
      )}
    </>
  );
};

export default Sales;