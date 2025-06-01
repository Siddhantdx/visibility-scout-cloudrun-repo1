import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery # Import BigQuery client

# Basic logging setup, focusing on INFO and ERROR for external visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Initialize BigQuery client globally for efficiency
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat") # <<< REPLACE WITH YOUR_PROJECT_ID
bigquery_client = bigquery.Client(project=project_id)
table_id = f"{project_id}.supply_chain_data1.raw_events" # <<< ENSURE THIS IS supply_chain_data1

@app.route("/", methods=["POST"])
def process_pubsub_message():
    try:
        # Get raw request body from Eventarc trigger
        raw_request_body = request.get_data()

        # Attempt to parse as JSON. If not valid, it will raise an error.
        data = request.get_json() 

        if not data or 'message' not in data:
            logging.error("Bad Request: Request body is missing or 'message' key is absent. Data: %s", raw_request_body)
            return "Bad Request: Missing data or 'message' key", 400

        pubsub_message = data['message']

        decoded_data = None
        if 'data' in pubsub_message and pubsub_message['data']:
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            logging.info("Decoded Pub/Sub message payload received.")
        else:
            logging.info("Pub/Sub message has no 'data' payload or it's empty.")
            return "OK - No data payload to process", 200 

        # Prepare data for BigQuery insertion
        row_to_insert = {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='microseconds') + 'Z' # Ensure ISO format with Z
        }

        message_json = None
        if decoded_data:
            try:
                message_json = json.loads(decoded_data)
                logging.info("Successfully parsed Pub/Sub message payload as JSON.")

                # Map fields to BigQuery schema, ensuring types match as much as possible
                row_to_insert["eventType"] = message_json.get("eventType", None)
                # For a JSON type column in BigQuery, pass the Python dictionary directly
                # or a JSON string. Passing dict is usually preferred if schema is JSON type.
                row_to_insert["data"] = json.dumps(message_json.get("data", {})) # Ensure it's a JSON string, default to empty dict if 'data' missing
                row_to_insert["source"] = message_json.get("source", None)

                raw_timestamp = message_json.get("timestamp", None)
                if raw_timestamp:
                    try:
                        # Attempt to parse ISO formats, BigQuery prefers Z for UTC
                        dt_object = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                        row_to_insert["timestamp"] = dt_object.isoformat(timespec='microseconds').replace('+00:00', 'Z')
                    except ValueError:
                        logging.warning(f"Invalid timestamp format: {raw_timestamp}. Skipping timestamp insertion.")
                        row_to_insert["timestamp"] = None
                else:
                    row_to_insert["timestamp"] = None

            except json.JSONDecodeError:
                logging.error("Failed to parse decoded Pub/Sub message payload as JSON. Cannot insert into BigQuery 'data' JSON column.")
                return "Error: Payload not valid JSON for BigQuery", 400

        # --- BigQuery Insertion ---
        logging.info("Attempting to insert row into BigQuery.")
        errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])

        if errors:
            logging.error(f"Errors inserting rows into BigQuery: {errors}")
            return "Internal Server Error: BigQuery insertion failed", 500
        else:
            logging.info("Data successfully inserted into BigQuery.")
        # --------------------------

    except Exception as e:
        # Catch any unexpected errors during processing
        logging.error(f"FATAL: Unhandled exception during message processing: {e}", exc_info=True)
        return "Internal Server Error during processing", 500

    logging.info("Visibility Scout processed message successfully (returned 200 OK).")
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
