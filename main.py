import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery # Import BigQuery client globally

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Initialize BigQuery client globally for efficiency, assuming it's not the cause of early crash
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat") # <<< REPLACE WITH YOUR_PROJECT_ID
bigquery_client = bigquery.Client(project=project_id)
table_id = f"{project_id}.supply_chain_data1.raw_events" # <<< ENSURE THIS IS supply_chain_data1

@app.route("/", methods=["POST"])
def process_pubsub_message():
    print("--- DEBUG: Visibility Scout: Request START ---") # First print

    raw_request_body = request.get_data()
    print(f"--- DEBUG: Raw request body (length {len(raw_request_body)} bytes): {raw_request_body[:200]}... ---") # Print truncated raw body

    try:
        print("--- DEBUG: Attempting to parse request body as JSON... ---")
        # Changed to NOT silent, so it will raise an error if not valid JSON
        # This error will be caught by the outer try-except block
        data = request.get_json() 
        print("--- DEBUG: Request body successfully parsed as JSON by Flask. ---")

        if data is None: # This check should now be redundant if get_json() raises an error for non-JSON
            print("--- ERROR: Parsed data is None. This should not happen after non-silent get_json(). ---")
            return "Bad Request: Parsed data is None", 400

        print(f"--- DEBUG: Checking for 'message' key in parsed data... ('message' in data: {'message' in data}) ---")
        if 'message' not in data:
            print("--- ERROR: 'message' key missing in parsed request body. ---")
            logging.error("No 'message' key found in the parsed request body. This is unexpected for Eventarc Pub/Sub trigger.")
            return "Bad Request: Missing 'message' key", 400

        pubsub_message = data['message']
        print(f"--- DEBUG: 'message' key extracted. Pub/Sub message ID: {pubsub_message.get('messageId')} ---")

        decoded_data = None
        if 'data' in pubsub_message and pubsub_message['data']:
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            logging.info(f"Decoded Pub/Sub message payload (length {len(decoded_data)} bytes): {decoded_data[:200]}...")
        else:
            logging.info("Pub/Sub message has no 'data' payload or it's empty.")
            return "OK - No data payload to process", 200 

        print("--- DEBUG: Attempting to parse decoded Pub/Sub payload as JSON... ---")
        if decoded_data:
            try:
                message_json = json.loads(decoded_data)
                logging.info(f"Successfully parsed Pub/Sub message payload as JSON: {json.dumps(message_json, indent=2)}")

                # Prepare data for BigQuery insertion
                row_to_insert = {
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
                }
                row_to_insert["eventType"] = message_json.get("eventType")
                row_to_insert["data"] = json.dumps(message_json.get("data")) # Convert data dict to JSON string for JSON type column
                row_to_insert["source"] = message_json.get("source")

                raw_timestamp = message_json.get("timestamp")
                if raw_timestamp:
                    try:
                        dt_object = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                        row_to_insert["timestamp"] = dt_object.isoformat(timespec='microseconds').replace('+00:00', 'Z')
                    except ValueError:
                        logging.warning(f"Invalid timestamp format: {raw_timestamp}. Skipping timestamp insertion.")
                        row_to_insert["timestamp"] = None
                else:
                    row_to_insert["timestamp"] = None

            except json.JSONDecodeError:
                print("--- ERROR: Decoded Pub/Sub payload NOT valid JSON. ---") # DEBUG
                logging.warning("Decoded Pub/Sub message payload is NOT a valid JSON string. Skipping JSON parsing.")
                logging.error("Failed to parse decoded Pub/Sub message payload as JSON. Cannot insert into BigQuery 'data' JSON column.")
                return "Error: Decoded payload not valid JSON for BigQuery", 400

        # --- BigQuery Insertion ---
        logging.info(f"Attempting to insert row into BigQuery: {json.dumps(row_to_insert)}")
        errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])

        if errors:
            print(f"--- ERROR: BigQuery insertion failed with errors: {errors} ---") # DEBUG
            logging.error(f"Errors inserting rows into BigQuery: {errors}")
            return "Internal Server Error: BigQuery insertion failed", 500
        else:
            print("--- DEBUG: Data successfully inserted into BigQuery. ---") # DEBUG
            logging.info("Data successfully inserted into BigQuery.")
        # --------------------------

    except Exception as e:
        # Catch any unexpected errors during processing
        print(f"--- FATAL ERROR: Unhandled exception caught: {e} ---") # DEBUG
        logging.error(f"FATAL: Unhandled exception during message processing: {e}", exc_info=True)
        return "Internal Server Error during processing", 500

    print("--- Visibility Scout: Request END (Returning OK) ---") # Final debug print
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
