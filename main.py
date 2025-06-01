import base64
import json
import logging
import os

from flask import Flask, request
from google.cloud import bigquery # Import BigQuery client

logging.basicConfig(level=logging.INFO) # Configure logging

app = Flask(__name__)

# Initialize BigQuery client outside the request handler for efficiency
# For prototype, we'll just print/log, but this sets up for future.
# bigquery_client = bigquery.Client()

@app.route("/", methods=["POST"])
def process_pubsub_message():
    """
    Cloud Run service to process Pub/Sub messages via Eventarc.
    This function acts as our Visibility Scout.
    It receives raw supply chain event data, decodes it, and logs it.
    """
    try:
        # Eventarc sends Pub/Sub messages in the request.json body
        # under a 'message' key, with 'data' and 'attributes'.
        # Use get_json(silent=True) to avoid immediate crash if not valid JSON
        data = request.get_json(silent=True)

        if not data:
            logging.error("Received request body is empty or not valid JSON.")
            return "Bad Request: Empty or invalid JSON body", 400

        logging.info(f"Received raw JSON request: {json.dumps(data, indent=2)}")

        if 'message' not in data:
            logging.error("No Pub/Sub message found in request body (missing 'message' key)")
            return "Bad Request: Missing Pub/Sub message key", 400

        pubsub_message = data['message']

        # Pub/Sub messages 'data' payload is base64 encoded. Decode it.
        if 'data' in pubsub_message and pubsub_message['data']: # Ensure 'data' exists and isn't empty
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            logging.info(f"Received and decoded Pub/Sub message payload: {decoded_data}")

            # Attempt to parse the decoded payload as JSON
            try:
                message_json = json.loads(decoded_data)
                logging.info(f"Parsed JSON message payload: {json.dumps(message_json, indent=2)}")

                # --- Placeholder for future BigQuery ingestion ---
                # In a real scenario, you would insert this standardized data into BigQuery here.
                # table_id = "your_project_id.your_dataset_id.your_table_id"
                # errors = bigquery_client.insert_rows_json(table_id, [message_json])
                # if errors:
                #     logging.error(f"Errors inserting rows into BigQuery: {errors}")
                # else:
                #     logging.info("Data successfully inserted into BigQuery.")
                # --------------------------------------------------

            except json.JSONDecodeError:
                logging.warning("Decoded Pub/Sub message payload is not a valid JSON string. Treating as plain text.")
                # If not JSON, log as plain text or handle differently
                logging.info(f"Pub/Sub message payload (plain text): {decoded_data}")

        else:
            logging.info("Pub/Sub message has no 'data' payload or it's empty.")

        if 'attributes' in pubsub_message:
            logging.info(f"Pub/Sub message attributes: {json.dumps(pubsub_message['attributes'], indent=2)}")

    except Exception as e:
        # Catch any unexpected errors during processing
        logging.error(f"Unhandled error during message processing: {e}", exc_info=True) # exc_info=True prints traceback
        return "Internal Server Error during processing", 500

    logging.info("Visibility Scout processed message successfully (returned 200 OK).")
    return "OK", 200 # Indicate successful processing

if __name__ == "__main__":
    # Cloud Run will typically run this via Gunicorn, so this block is for local testing.
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
