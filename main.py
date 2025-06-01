import base64
import json
import logging
import os

from flask import Flask, request

logging.basicConfig(level=logging.INFO) # Configure logging

app = Flask(__name__)

@app.route("/", methods=["POST"])
def process_pubsub_message():
    """
    Cloud Run service to process Pub/Sub messages via Eventarc.
    This function acts as our Visibility Scout.
    It receives raw supply chain event data, decodes it, and logs it.
    """
    if not request.is_json:
        logging.error("Request must be JSON")
        return "Bad Request", 400

    data = request.get_json()
    logging.info(f"Received JSON request: {json.dumps(data, indent=2)}")

    if not data or 'message' not in data:
        logging.error("No Pub/Sub message found in request body")
        return "Bad Request: No message", 400

    pubsub_message = data['message']

    try:
        if 'data' in pubsub_message:
            decoded_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
            logging.info(f"Received and decoded Pub/Sub message payload: {decoded_data}")

            try:
                message_json = json.loads(decoded_data)
                logging.info(f"Parsed JSON message: {json.dumps(message_json, indent=2)}")
                # Here, in a real scenario, you would push this data to BigQuery.

            except json.JSONDecodeError:
                logging.warning("Received message payload is not a valid JSON string.")
        else:
            logging.info("Pub/Sub message has no 'data' payload.")

        if 'attributes' in pubsub_message:
            logging.info(f"Pub/Sub message attributes: {json.dumps(pubsub_message['attributes'], indent=2)}")

    except Exception as e:
        logging.error(f"Error decoding or processing message: {e}")
        logging.error(f"Raw Pub/Sub message data: {pubsub_message}")
        return "Internal Server Error", 500

    logging.info("Visibility Scout processed message successfully.")
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
