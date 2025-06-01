import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery 

# We can't see these logs, but keep them for robustness if logging ever gets fixed
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Initialize BigQuery client globally for efficiency
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat") # <<< REPLACE WITH YOUR_PROJECT_ID
bigquery_client = bigquery.Client(project=project_id)
table_id = "wycfots-agbgagenticaihackat.supply_chain_data1.raw_events" # <<< ENSURE THIS IS supply_chain_data1

@app.route("/", methods=["POST"])
def process_pubsub_message():
    # Signal: Request received, started execution
    # print("--- DEBUG: Visibility Scout: Request START ---") # Won't show in logs

    try:
        raw_request_body = request.get_data()
        # print(f"Raw request body received (length {len(raw_request_body)} bytes): {raw_request_body[:200]}... ---") # Won't show in logs

        # Signal: Attempting to parse JSON
        # print("--- DEBUG: Attempting to parse request body as JSON... ---") # Won't show in logs
        data = request.get_json() # Non-silent: will raise error if not valid JSON

        # Signal: Request body parsed as JSON by Flask
        # print("--- DEBUG: Request body successfully parsed as JSON by Flask. ---") # Won't show in logs

        if not data: # Should not happen if get_json() raises an error
            return "Bad Request: Empty or invalid JSON body after parsing", 400

        # Signal: Checking for 'message' key
        # print(f"--- DEBUG: Checking for 'message' key: {'message' in data} ---") # Won't show in logs
        if 'message' not in data:
            return "Bad Request: Missing 'message' key in Eventarc payload", 400

        pubsub_message = data['message']
        # print(f"--- DEBUG: 'message' key extracted. Pub/Sub message ID: {pubsub_message.get('messageId')} ---") # Won't show in logs

        decoded_data = None
        if 'data' in pubsub_message and pubsub_message['data']:
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            # logging.info("Decoded Pub/Sub message payload received.") # Won't show in logs
        else:
            # Signal: No data payload in Pub/Sub message
            return "OK - No data payload to process", 200 # Still return OK as it's not an error condition

        # Signal: Attempting to parse decoded Pub/Sub payload as JSON
        # print("--- DEBUG: Attempting to parse decoded Pub/Sub payload as JSON... ---") # Won't show in logs
        if decoded_data:
            try:
                message_json = json.loads(decoded_data)
                # logging.info("Successfully parsed Pub/Sub message payload as JSON.") # Won't show in logs

                # Prepare data for BigQuery insertion - ensure data types match schema
                row_to_insert = {
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='microseconds') + 'Z'
                }
                row_to_insert["eventType"] = message_json.get("eventType", None)
                row_to_insert["data"] = json.dumps(message_json.get("data", {})) # Convert dict to JSON string for BigQuery JSON column
                row_to_insert["source"] = message_json.get("source", None)

                raw_timestamp = message_json.get("timestamp", None)
                if raw_timestamp:
                    try:
                        dt_object = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                        row_to_insert["timestamp"] = dt_object.isoformat(timespec='microseconds').replace('+00:00', 'Z')
                    except ValueError:
                        # logging.warning(f"Invalid timestamp format: {raw_timestamp}. Skipping timestamp insertion.") # Won't show
                        row_to_insert["timestamp"] = None
                else:
                    row_to_insert["timestamp"] = None

            except json.JSONDecodeError:
                # Signal: Decoded Pub/Sub payload not valid JSON
                return "Error: Decoded Pub/Sub payload not valid JSON", 400

        # --- BigQuery Insertion ---
        # logging.info("Attempting to insert row into BigQuery.") # Won't show
        errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])

        if errors:
            # Signal: BigQuery insertion failed with errors
            return "Internal Server Error: BigQuery insertion failed (errors returned)", 501 # Custom 501 for specific BigQuery error
        else:
            # Signal: Data successfully inserted into BigQuery.
            return "OK - Data ingested to BigQuery", 202 # Custom 202 for success

    except Exception as e:
        # Signal: Unhandled exception caught
        # logging.error(f"FATAL: Unhandled exception caught: {e}", exc_info=True) # Won't show
        return "Internal Server Error: Unhandled exception", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
