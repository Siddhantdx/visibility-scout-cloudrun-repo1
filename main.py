import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from google.cloud import bigquery

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

# Global BigQuery setup
project_id = os.environ.get("GCP_PROJECT_ID", "wycfots-agbgagenticaihackat")
bigquery_client = bigquery.Client(project=project_id)
table_id = f"{project_id}.supply_chain_data1.raw_events"

@app.route("/", methods=["POST"])
def process_pubsub_message():
    try:
        data = request.get_json()
        raw_request_body = request.get_data()
        logging.info("Received request body: %s", raw_request_body)

        if not data or "message" not in data:
            logging.error("Missing 'message' key in request.")
            return "Bad Request", 400

        message_str = data["message"]
        if not isinstance(message_str, str):
            logging.error("Expected 'message' to be a string.")
            return "Bad Request", 400

        try:
            json_start = message_str.index("{")
            message_json_str = message_str[json_start:]
            payload = json.loads(message_json_str)
            logging.info("Parsed JSON payload: %s", json.dumps(payload))
        except Exception as e:
            logging.error(f"Failed to extract or parse message JSON: {e}")
            return "Invalid Payload", 400

        # Prepare row for BigQuery
        row = {
            "eventType": payload.get("eventType"),
            "source": payload.get("source"),
            "data": json.dumps(payload.get("data", {})),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds") + "Z"
        }

        raw_timestamp = payload.get("data", {}).get("timestamp")
        if raw_timestamp:
            try:
                dt_obj = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
                row["timestamp"] = dt_obj.isoformat(timespec="microseconds").replace("+00:00", "Z")
            except ValueError:
                logging.warning("Invalid timestamp in payload: %s", raw_timestamp)
                row["timestamp"] = None
        else:
            row["timestamp"] = None

        # Insert row
        logging.info("Inserting into BigQuery: %s", json.dumps(row))
        errors = bigquery_client.insert_rows_json(table_id, [row])
        if errors:
            logging.error("BigQuery insertion error: %s", errors)
            return "Insertion Failed", 500

        logging.info("Insert successful.")
        return "OK", 200

    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        return "Internal Error", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
