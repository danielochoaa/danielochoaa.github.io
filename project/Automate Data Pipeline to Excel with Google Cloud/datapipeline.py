#Pipeline Overview
# 1. Data Extraction: Pull data from multiple sources (APIs, databases, cloud stroage)
# 2. Data Transformation: Process and clean the data
# 3. Excel Generation: Create formatted Excel files
# 4. Cloud Integration: Utilize Google Cloud for storage and processing

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
from datetime import datetime
import requests  # for API sources

class ExcelAutomationPipeline:
    def __init__(self, gcp_credentials_path):
        # Initialize GCP clients
        self.credentials = service_account.Credentials.from_service_account_file(
            gcp_credentials_path
        )
        self.storage_client = storage.Client(credentials=self.credentials)
        
    def fetch_data_from_api(self, api_url, params=None):
        """Fetch data from REST API"""
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None
    
    def read_from_gcs(self, bucket_name, file_name):
        """Read data from Google Cloud Storage"""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Handle different file types
        if file_name.endswith('.csv'):
            data = blob.download_as_text()
            return pd.read_csv(io.StringIO(data))
        elif file_name.endswith('.json'):
            data = blob.download_as_text()
            return pd.read_json(io.StringIO(data))
        else:
            print(f"Unsupported file format: {file_name}")
            return None
    
    def transform_data(self, df, transformations):
        """Apply data transformations"""
        # Example transformations:
        # - Clean data
        # - Convert types
        # - Apply business logic
        if transformations.get('rename_columns'):
            df = df.rename(columns=transformations['rename_columns'])
        
        if transformations.get('date_columns'):
            for col in transformations['date_columns']:
                df[col] = pd.to_datetime(df[col])
        
        return df
    
    def create_excel_with_sheets(self, data_dict, output_path):
        """Create Excel file with multiple sheets"""
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Excel file created at {output_path}")
    
    def upload_to_gcs(self, file_path, bucket_name, destination_blob_name):
        """Upload file to Google Cloud Storage"""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(file_path)
        print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
    
    def run_pipeline(self, config):
        """Orchestrate the entire pipeline"""
        all_data = {}
        
        # Step 1: Extract data from various sources
        for source in config['sources']:
            if source['type'] == 'api':
                data = self.fetch_data_from_api(source['url'], source.get('params'))
                if data:
                    df = pd.DataFrame(data[source['data_key']])
                    all_data[source['name']] = df
            
            elif source['type'] == 'gcs':
                df = self.read_from_gcs(source['bucket'], source['file'])
                if df is not None:
                    all_data[source['name']] = df
        
        # Step 2: Transform data
        for name, df in all_data.items():
            transformations = next(
                (s['transformations'] for s in config['sources'] if s['name'] == name
            ), {})
            all_data[name] = self.transform_data(df, transformations)
        
        # Step 3: Generate Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{config['output']['base_name']}_{timestamp}.xlsx"
        self.create_excel_with_sheets(all_data, output_filename)
        
        # Step 4: Upload to GCS if configured
        if config['output'].get('gcs_upload'):
            self.upload_to_gcs(
                output_filename,
                config['output']['gcs_upload']['bucket'],
                f"{config['output']['gcs_upload']['path']}/{output_filename}"
            )
        
        return output_filename

if __name__ == "__main__":
    # Initialize the pipeline with your GCP credentials
    pipeline = ExcelAutomationPipeline('path/to/your/service-account-key.json')
    
    # Load configuration
    import json
    with open('pipeline_config.json') as f:
        config = json.load(f)
    
    # Run the pipeline
    output_file = pipeline.run_pipeline(config)
    print(f"Pipeline completed. Output file: {output_file}")

