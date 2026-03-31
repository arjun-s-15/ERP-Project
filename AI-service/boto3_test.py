import boto3

s3 = boto3.client('s3')
s3.upload_file('/Users/atharv/MLProjects/ERP-Project/AI-service/sample_data/online_retail_09_10.csv', 'insighto-s3-bucket', 'sample_sales.csv')