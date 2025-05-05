import requests
import pandas as pd
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
from datetime import datetime
import traceback

# Configuration
PDF_FOLDER_PATH = r"/Users/jijashshrestha/Downloads"  # Folder to monitor for new PDFs
DOCUMENT_EXTRACT_API_URL = "https://a800ddf2-e1c4-4f7f-8060-aba95fcb6307.mock.pstmn.io/document_extract"  # Replace with actual API URL
ENTITY_EXTRACTOR_API_URL = "https://a800ddf2-e1c4-4f7f-8060-aba95fcb6307.mock.pstmn.io/entity_extraction"  # Replace with actual API URL
API_KEY = "mock-api-key"  # Replace with your API key
BASE_OUTPUT_FOLDER = r"/Users/jijashshrestha/Downloads/excel_outputs"  # Base folder for all outputs
LOG_FILE = r"/Users/jijashshrestha/Downloads/test_logging.txt"  # Path to log file

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Step 1: Call Document Extract API
def call_document_extract_api(pdf_path):
    start_time = time.time()
    logger.info(f"Starting Document Extract API call for {pdf_path} to {DOCUMENT_EXTRACT_API_URL}")
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }
    
    try:
        with open(pdf_path, "rb") as pdf_file:
            files = {"file": (os.path.basename(pdf_path), pdf_file, "application/pdf")}
            response = requests.post(DOCUMENT_EXTRACT_API_URL, headers=headers, files=files)
            response.raise_for_status()
            response_time = time.time() - start_time
            logger.info(
                f"Document Extract API call successful for {pdf_path}. "
                f"Status: {response.status_code}, Response Time: {response_time:.2f}s"
            )
            return response.json()
    except requests.exceptions.RequestException as e:
        response_time = time.time() - start_time
        logger.error(
            f"Document Extract API call failed for {pdf_path}. "
            f"Status: {getattr(e.response, 'status_code', 'N/A')}, "
            f"Response Time: {response_time:.2f}s, Error: {str(e)}"
        )
        logger.debug(f"Stack trace: {traceback.format_exc()}")
        return None

# Step 2: Call Entity Extractor API
def call_entity_extractor_api(extracted_data):
    start_time = time.time()
    logger.info(f"Starting Entity Extractor API call to {ENTITY_EXTRACTOR_API_URL}")
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "text": extracted_data.get("extracted_text", "")
    }
    
    try:
        response = requests.post(ENTITY_EXTRACTOR_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        response_time = time.time() - start_time
        logger.info(
            f"Entity Extractor API call successful. "
            f"Status: {response.status_code}, Response Time: {response_time:.2f}s"
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        response_time = time.time() - start_time
        logger.error(
            f"Entity Extractor API call failed. "
            f"Status: {getattr(e.response, 'status_code', 'N/A')}, "
            f"Response Time: {response_time:.2f}s, Error: {str(e)}"
        )
        logger.debug(f"Stack trace: {traceback.format_exc()}")
        return None

# Step 3: Process a single PDF
def process_pdf(pdf_path):
    logger.info(f"Processing new PDF: {pdf_path}")
    
    # Call Document Extract API
    doc_extract_response = call_document_extract_api(pdf_path)
    if not doc_extract_response:
        logger.error(f"Skipping {pdf_path} due to Document Extract API failure")
        return None
    
    # Get document type from the Document Extract API response
    document_type = doc_extract_response.get("document_type", "").lower()
    
    # Call Entity Extractor API
    entity_extract_response = call_entity_extractor_api(doc_extract_response)
    if not entity_extract_response:
        logger.error(f"Skipping {pdf_path} due to Entity Extractor API failure")
        return None
    
    # Initialize document type lists
    ap_invoice = []
    outgoing_payments = []
    incoming_payments = []
    
    # Build entity structure
    entity = {
        "file_name": os.path.basename(pdf_path),
        "vendor_details": entity_extract_response.get("vendor_details", {}),
        "customer_details": entity_extract_response.get("customer_details", {}),
        "invoice_details": entity_extract_response.get("invoice_details", {}),
        "line_items": entity_extract_response.get("line_items", []),
        # "file_name": os.path.basename(pdf_path)
    }
    
    # Append to appropriate list based on document type
    if document_type == "ap_invoice":
        ap_invoice.append(entity)
    elif document_type == "outgoing_payments":
        outgoing_payments.append(entity)
    elif document_type == "incoming_payments":
        incoming_payments.append(entity)
    else:
        logger.warning(f"Unknown document type: {document_type}")

    # Return all lists with their respective entities
    return {
        "ap_invoice": ap_invoice,
        "outgoing_payments": outgoing_payments,
        "incoming_payments": incoming_payments
    }

# Step 4: Append to Excel
def append_to_excel(entities, base_output_folder):
    try:
        if not entities:
            logger.warning("No entities to save")
            return
        
        # Create daily folder with timestamp
        today = datetime.now().strftime("%Y-%m-%d")
        daily_folder = os.path.join(base_output_folder, f"excel_output_{today}")
        os.makedirs(daily_folder, exist_ok=True)
        
        # Process each document type separately
        for doc_type, doc_entities in entities.items():
            if not doc_entities:  # Skip if no entities for this type
                continue
                
            # Create a list to store flattened entities
            flattened_entities = []
            
            for entity in doc_entities:
                flattened = {
                    'document_type': doc_type,  # Add document_type as first column
                    'file_name': entity['file_name']
                }
                
                # Flatten vendor_details
                if 'vendor_details' in entity:
                    for key, value in entity['vendor_details'].items():
                        flattened[f'vendor_{key}'] = value
                
                # Flatten customer_details
                if 'customer_details' in entity:
                    for key, value in entity['customer_details'].items():
                        flattened[f'customer_{key}'] = value
                
                # Flatten invoice_details
                if 'invoice_details' in entity:
                    for key, value in entity['invoice_details'].items():
                        flattened[f'invoice_{key}'] = value
                
                # Handle line_items
                if 'line_items' in entity and entity['line_items']:
                    # Take the first line item and flatten it
                    first_item = entity['line_items'][0]
                    for key, value in first_item.items():
                        flattened[f'line_item_{key}'] = value
                
                flattened_entities.append(flattened)
            
            if not flattened_entities:
                continue
                
            # Create DataFrame for this document type
            df_new = pd.DataFrame(flattened_entities)
            
            # Add empty rows for spacing
            spacing_rows = pd.DataFrame([{} for _ in range(2)])  # Add 2 empty rows
            df_new = pd.concat([df_new, spacing_rows], ignore_index=True)
            
            # Define output path for this document type
            output_path = os.path.join(daily_folder, f"{doc_type}_output.xlsx")
            
            # If Excel file exists, append; otherwise, create new
            if os.path.exists(output_path):
                df_existing = pd.read_excel(output_path, engine="openpyxl")
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new
            
            # Save to Excel
            df_combined.to_excel(output_path, index=False, engine="openpyxl")
            logger.info(f"Successfully appended {len(df_new)} rows to {output_path}")
            
    except Exception as e:
        logger.error(f"Error appending to Excel: {str(e)}")
        logger.debug(f"Stack trace: {traceback.format_exc()}")

# File System Event Handler
class PdfFileHandler(FileSystemEventHandler):
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.processed_files = set()
    
    def process_file(self, file_path):
        if not file_path.lower().endswith(".pdf"):
            return

        if not self.is_file_ready(file_path):
            return

        if file_path in self.processed_files:
            return

        self.processed_files.add(file_path)
        logging.info(f"New PDF detected (processed): {file_path}")

        entities = process_pdf(file_path)
        if entities:
            append_to_excel(entities, BASE_OUTPUT_FOLDER)

    def on_created(self, event):
        if event.is_directory:
            return
        self.process_file(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self.process_file(event.dest_path)

    def on_modified(self, event):
        """
        Optional: Sometimes, downloads only trigger modified events.
        Be cautious to avoid re-processing the same file multiple times.
        """
        if event.is_directory:
            return
        # You might decide to enable this ONLY if needed
        # self.process_file(event.src_path)

    def is_file_ready(self, file_path, timeout=30, check_interval=1):
        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(file_path)
                if current_size == last_size:
                    return True
                last_size = current_size
                time.sleep(check_interval)
            except (OSError, FileNotFoundError):
                return False

        logging.warning(f"Timeout waiting for {file_path} to be ready")
        return False

# Main Workflow
def main():
    # Verify folder exists
    if not os.path.exists(PDF_FOLDER_PATH):
        logger.error(f"Folder not found at {PDF_FOLDER_PATH}")
        print(f"Folder not found at {PDF_FOLDER_PATH}")
        return
    
    # Set up file system watcher
    event_handler = PdfFileHandler(PDF_FOLDER_PATH)
    observer = Observer()
    observer.schedule(event_handler, PDF_FOLDER_PATH, recursive=False)
    
    # Start the observer
    observer.start()
    logger.info(f"Started monitoring {PDF_FOLDER_PATH}")
    print(f"Monitoring {PDF_FOLDER_PATH} for new PDF files. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopped monitoring")
        print("Monitoring stopped.")
    
    observer.join()

if __name__ == "__main__":
    main()