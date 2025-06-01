import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery # Import BigQuery client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Initialize BigQuery client (outside the request handler for efficiency)
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat") # <<< REPLACE WITH YOUR_PROJECT_ID
bigquery_client = bigquery.Client(project=project_id)

# Define your BigQuery table ID
table_id = f"{project_id}.supply_chain_data1.raw_events"

@app.route("/", methods=["POST"])
def process_pubsub_message():
    """
    Cloud Run service to process Pub/Sub messages via Eventarc and ingest into BigQuery.
    """
    print("--- Visibility Scout: Incoming Request Received ---") # DEBUG

    raw_request_body = request.get_data()
    print(f"Raw request body received (length {len(raw_request_body)} bytes): {raw_request_body}") # DEBUG

    try:
        data = request.get_json(silent=True)

        if data is None:
            print("--- ERROR: Request body is NOT valid JSON or empty. ---") # DEBUG
            logging.error("Received request body is not valid JSON as expected from Eventarc.")
            return "Bad Request: Expected JSON body", 400

        print(f"Parsed initial JSON request: {json.dumps(data, indent=2)}") # DEBUG

        if 'message' not in data:
            print("--- ERROR: No 'message' key found in parsed request. ---") # DEBUG
            logging.error("No 'message' key found in the parsed request body. This is unexpected for Eventarc Pub/Sub trigger.")
            return "Bad Request: Missing 'message' key", 400

        pubsub_message = data['message']

        decoded_data = None
        if 'data' in pubsub_message and pubsub_message['data']:
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            logging.info(f"Decoded Pub/Sub message payload: {decoded_data}")
        else:
            logging.info("Pub/Sub message has no 'data' payload or it's empty.")
            return "OK - No data payload to process", 200 

        # Prepare data for BigQuery insertion
        row_to_insert = {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
        }

        if decoded_data:
            try:
                message_json = json.loads(decoded_data)
                logging.info(f"Successfully parsed Pub/Sub message payload as JSON: {json.dumps(message_json, indent=2)}")

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
                logging.warning("Decoded Pub/Sub message payload is NOT a valid JSON string. Skipping JSON parsing.")
                logging.error("Failed to parse decoded Pub/Sub message payload as JSON. Cannot insert into BigQuery 'data' JSON column.")
                return "Error: Payload not valid JSON for BigQuery", 400

        # --- BigQuery Insertion ---
        logging.info(f"Attempting to insert row into BigQuery: {json.dumps(row_to_insert)}")
        errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])

        if errors:
            logging.error(f"Errors inserting rows into BigQuery: {errors}")
            return "Internal Server Error: BigQuery insertion failed", 500
        else:
            logging.info("Data successfully inserted into BigQuery.")
        # --------------------------

    except Exception as e:
        logging.error(f"FATAL: Unhandled exception during message processing: {e}", exc_info=True)
        return "Internal Server Error during processing", 500

    print("--- Visibility Scout: Successfully Processed Message & Attempted BigQuery Insertion ---") # DEBUG
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
