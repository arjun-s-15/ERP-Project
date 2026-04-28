import Layout from "../components/Layout";
import FileUpload from "../components/FileUpload";

const Sales = () => {
  const handleFileSelect = (file) => {
    console.log("Selected file:", file.name);
  };

  return (
    <Layout>
      <h1>Sales</h1>
      <FileUpload onFileSelect={handleFileSelect} />
    </Layout>
  );
};

export default Sales;
