import requests
import pandas as pd
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

# Configuration
PDF_FOLDER_PATH = r"/Users/jijashshrestha/Downloads"  # Folder to monitor for new PDFs
DOCUMENT_EXTRACT_API_URL = "https://a800ddf2-e1c4-4f7f-8060-aba95fcb6307.mock.pstmn.io/document_extract"  # Replace with actual API URL
ENTITY_EXTRACTOR_API_URL = "https://a800ddf2-e1c4-4f7f-8060-aba95fcb6307.mock.pstmn.io/entity_extraction"  # Replace with actual API URL
API_KEY = "mock-api-key"  # Replace with your API key
OUTPUT_EXCEL_PATH = r"/Users/jijashshrestha/Downloads/test_output.xlsx"  # Path to save the combined Excel file
LOG_FILE = r"/Users/jijashshrestha/Downloads/test_logging.txt"  # Path to log file

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Step 1: Call Document Extract API
def call_document_extract_api(pdf_path):
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }
    
    try:
        with open(pdf_path, "rb") as pdf_file:
            files = {"file": (os.path.basename(pdf_path), pdf_file, "application/pdf")}
            response = requests.post(DOCUMENT_EXTRACT_API_URL, headers=headers, files=files)
            response.raise_for_status()
            return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Document Extract API error for {pdf_path}: {e}")
        return None

# Step 2: Call Entity Extractor API
def call_entity_extractor_api(extracted_data):
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
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Entity Extractor API error: {e}")
        return None

# Step 3: Process a single PDF
def process_pdf(pdf_path):
    logging.info(f"Processing {pdf_path}")
    
    # Call Document Extract API
    doc_extract_response = call_document_extract_api(pdf_path)
    if not doc_extract_response:
        logging.error(f"Skipping {pdf_path} due to Document Extract API failure")
        return None
    
    # Call Entity Extractor API
    entity_extract_response = call_entity_extractor_api(doc_extract_response)
    if not entity_extract_response:
        logging.error(f"Skipping {pdf_path} due to Entity Extractor API failure")
        return None
    
    # Extract entities and add PDF filename
    entities = entity_extract_response.get("entities", [])
    for entity in entities:
        entity["pdf_file"] = os.path.basename(pdf_path)
    
    return entities

# Step 4: Append to Excel
def append_to_excel(entities, output_path):
    try:
        if not entities:
            logging.warning("No entities to save")
            return
        
        # Convert entities to DataFrame
        df_new = pd.DataFrame(entities)
        
        # If Excel file exists, append; otherwise, create new
        if os.path.exists(output_path):
            df_existing = pd.read_excel(output_path, engine="openpyxl")
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        
        # Save to Excel
        df_combined.to_excel(output_path, index=False, engine="openpyxl")
        logging.info(f"Appended data to {output_path}")
    except Exception as e:
        logging.error(f"Error appending to Excel: {e}")

# File System Event Handler
# class PdfFileHandler(FileSystemEventHandler):
#     def __init__(self, folder_path):
#         self.folder_path = folder_path
#         self.processed_files = set()  # Track processed files to avoid duplicates
    
#     def on_created(self, event):
#         # Handle new file creation
#         if event.is_directory:
#             return
        
#         file_path = event.src_path
#         if not file_path.lower().endswith(".pdf"):
#             return
        
#         # Wait until the file is fully written
#         if not self.is_file_ready(file_path):
#             return
        
#         # Avoid processing the same file multiple times
#         if file_path in self.processed_files:
#             return
        
#         self.processed_files.add(file_path)
#         logging.info(f"New PDF detected: {file_path}")
        
#         # Process the PDF
#         entities = process_pdf(file_path)
#         if entities:
#             append_to_excel(entities, OUTPUT_EXCEL_PATH)
    
#     def is_file_ready(self, file_path, timeout=30, check_interval=1):
#         """
#         Check if the file is fully written by monitoring its size.
#         Returns True if the file is stable, False if timeout is reached.
#         """
#         start_time = time.time()
#         last_size = -1
        
#         while time.time() - start_time < timeout:
#             try:
#                 current_size = os.path.getsize(file_path)
#                 if current_size == last_size:
#                     return True
#                 last_size = current_size
#                 time.sleep(check_interval)
#             except (OSError, FileNotFoundError):
#                 return False
        
#         logging.warning(f"Timeout waiting for {file_path} to be ready")
#         return False
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
            append_to_excel(entities, OUTPUT_EXCEL_PATH)

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
        logging.error(f"Folder not found at {PDF_FOLDER_PATH}")
        print(f"Folder not found at {PDF_FOLDER_PATH}")
        return
    
    # Set up file system watcher
    event_handler = PdfFileHandler(PDF_FOLDER_PATH)
    observer = Observer()
    observer.schedule(event_handler, PDF_FOLDER_PATH, recursive=False)
    
    # Start the observer
    observer.start()
    logging.info(f"Started monitoring {PDF_FOLDER_PATH}")
    print(f"Monitoring {PDF_FOLDER_PATH} for new PDF files. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Stopped monitoring")
        print("Monitoring stopped.")
    
    observer.join()

if __name__ == "__main__":
    main()
