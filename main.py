import os
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def hello_world_post():
    """
    Super minimal Flask app to test Cloud Run POST functionality.
    It will just print confirmation of receiving a POST request.
    """
    print("--- DEBUG: Basic POST Request Received ---")
    print(f"--- DEBUG: Request Headers: {request.headers} ---")
    print(f"--- DEBUG: Request Method: {request.method} ---")

    # Try to read raw data, but don't attempt JSON parsing yet
    try:
        raw_data = request.get_data()
        print(f"--- DEBUG: Raw Request Data Length: {len(raw_data)} bytes ---")
        # print(f"--- DEBUG: Raw Request Data: {raw_data.decode('utf-8')} ---") # Uncomment for full data if desired
    except Exception as e:
        print(f"--- DEBUG ERROR: Failed to get raw data: {e} ---")

    print("--- DEBUG: Basic POST Request Processing Complete ---")
    return "OK - Minimal Test Successful", 200 # Always return 200 OK

if __name__ == "__main__":
    # Run the Flask app. Cloud Run uses Gunicorn by default.
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
