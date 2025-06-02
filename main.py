import base64
import json
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery
from google.cloud import logging as cloud_logging # Import Cloud Logging client

# Initialize Cloud Logging client directly
logging_client = cloud_logging.Client()
logger = logging_client.logger('visibility-scout-logger') # Use a specific logger name

app = Flask(__name__)

# Initialize BigQuery client globally for efficiency
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat")
bigquery_client = bigquery.Client(project=project_id)
table_id = f"{project_id}.supply_chain_data1.raw_events" # <<< ENSURE THIS IS supply_chain_data1

@app.route("/", methods=["POST"])
def process_pubsub_message():
    logger.info("--- (LOG:START) Visibility Scout: Request START ---") # Log start of request

    try:
        raw_request_body = request.get_data()
        logger.info(f"Raw request body (length {len(raw_request_body)} bytes): {raw_request_body[:200]}... ---") # Log raw body

        try:
            data = request.get_json() # Non-silent: will raise error if not valid JSON
            logger.info("Request body successfully parsed as JSON by Flask.") # Log parsing success

            if not data: 
                response_message = "Bad Request: Empty or invalid JSON body after parsing."
                logger.error(response_message)
                return response_message, 400 # 400 A

            if 'message' not in data:
                response_message = "Bad Request: Missing 'message' key in Eventarc payload."
                logger.error(response_message)
                return response_message, 400 # 400 B

            pubsub_message = data['message']
            logger.info(f"'message' key extracted. Pub/Sub message ID: {pubsub_message.get('messageId')}")

            decoded_data = None
            if 'data' in pubsub_message and pubsub_message['data']:
                decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
                logger.info("Decoded Pub/Sub message payload received.")
            else:
                response_message = "OK - No data payload to process"
                logger.info(response_message)
                return response_message, 200 # OK

            # Prepare data for BigQuery insertion
            row_to_insert = {
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec='microseconds') + 'Z'
            }

            message_json = None
            if decoded_data:
                try:
                    message_json = json.loads(decoded_data)
                    logger.info("Successfully parsed Pub/Sub message payload as JSON for insertion.")

                    row_to_insert["eventType"] = message_json.get("eventType", None)
                    row_to_insert["data"] = message_json.get("data", {}) # Pass as Python dict for BigQuery JSON type column
                    row_to_insert["source"] = message_json.get("source", None)

                    raw_timestamp = message_json.get("timestamp", None)
                    if raw_timestamp:
                        try:
                            dt_object = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                            row_to_insert["timestamp"] = dt_object.isoformat(timespec='microseconds').replace('+00:00', 'Z')
                        except ValueError:
                            response_message = f"Invalid timestamp format: {raw_timestamp}. Skipping timestamp insertion."
                            logger.warning(response_message)
                            row_to_insert["timestamp"] = None
                    else:
                        row_to_insert["timestamp"] = None

                except json.JSONDecodeError:
                    response_message = "Error: Decoded Pub/Sub payload not valid JSON for message_json."
                    logger.error(response_message)
                    return response_message, 400 # 400 C
            else:
                response_message = "Error: Decoded data is empty for insertion."
                logger.error(response_message)
                return response_message, 400 # 400 D

            # --- BigQuery Insertion ---
            logger.info("Attempting to insert row into BigQuery.")
            errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])

            if errors:
                response_message = f"Internal Server Error: BigQuery insertion failed (errors: {errors})"
                logger.error(response_message)
                return response_message, 501 # Custom 501 for specific BigQuery error
            else:
                response_message = "OK - Data ingested to BigQuery"
                logger.info(response_message)
                return response_message, 202 # Custom 202 for success

        except Exception as e:
            # Catch any unexpected errors during processing
            error_details = f"Unhandled exception: {e}"
            logger.error(f"FATAL: {error_details}", exc_info=True)
            return f"Internal Server Error: {error_details}", 500

    finally:
        logger.info("--- (LOG:END) Visibility Scout: Request END ---") # Log end of request
        logging_client.flush() # Attempt to flush logs immediately

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
