import base64
import json
import logging
import os

from flask import Flask, request

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# TEMPORARILY REMOVE BIGQUERY CLIENT INIT FOR DEBUGGING
# project_id = os.environ.get("GCP_PROJECT_ID", "wycfotos-agbgagenticaihackat")
# bigquery_client = bigquery.Client(project=project_id)
# table_id = f"{project_id}.supply_chain_data.raw_events"

@app.route("/", methods=["POST"])
def process_pubsub_message():
    print("--- DEBUG: Visibility Scout: Request START ---") # Very first print

    try:
        raw_request_body = request.get_data()
        print(f"--- DEBUG: Raw body length: {len(raw_request_body)} bytes ---") # Only print length

        data = request.get_json(silent=True) # Try silent parsing

        if data is None:
            print("--- DEBUG ERROR: Request body NOT valid JSON for get_json() ---")
            return "Bad Request: Body not JSON", 400

        print("--- DEBUG: Request body successfully parsed as JSON by Flask ---") # Confirm Flask parsing
        print(f"--- DEBUG: Message key check: {'message' in data} ---") # Check for 'message' key

        if 'message' not in data:
            print("--- DEBUG ERROR: 'message' key missing in parsed data ---")
            return "Bad Request: Missing 'message' key", 400

        pubsub_message = data['message']

        # Print base64 encoded data, not decoded
        if 'data' in pubsub_message and pubsub_message['data']:
            print(f"--- DEBUG: Pub/Sub data base64 length: {len(pubsub_message['data'])} ---")
            # Attempt to decode and parse as JSON
            try:
                decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
                message_json = json.loads(decoded_data)
                print("--- DEBUG: Pub/Sub payload successfully decoded and parsed as JSON ---")
                print(f"--- DEBUG: Parsed eventType: {message_json.get('eventType')} ---") # Print a specific field

                # --- TEMPORARY: Skip BigQuery Insertion for Debugging ---
                # errors = bigquery_client.insert_rows_json(table_id, [row_to_insert])
                # if errors: logging.error("BigQuery insertion failed", exc_info=True)
                # else: logging.info("Data successfully inserted into BigQuery.")

            except (base64.binascii.Error, UnicodeDecodeError) as decode_e:
                print(f"--- DEBUG ERROR: Base64 decode/Unicode error: {decode_e} ---")
                return "Error: Base64 decode error", 400
            except json.JSONDecodeError:
                print("--- DEBUG ERROR: Decoded Pub/Sub payload NOT valid JSON ---")
                return "Error: Decoded payload not JSON", 400
        else:
            print("--- DEBUG: Pub/Sub message has no 'data' payload ---")

    except Exception as e:
        print(f"--- DEBUG FATAL ERROR: Unhandled exception: {e} ---") # Final catch-all print
        logging.error(f"FATAL: Unhandled exception: {e}", exc_info=True)
        return "Internal Server Error during processing", 500

    print("--- DEBUG: Visibility Scout: Request END (Returning OK) ---") # Final print
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
