import os
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def super_minimal_post_test():
    """
    Absolute bare-bones Flask app to test Cloud Run POST functionality.
    It will just print a fixed string.
    """
    print("--- DEBUG: ULTRA BASIC POST RECEIVED ---") # Only print a fixed string
    return "OK - Ultra Minimal Test", 200

if __name__ == "__main__":
    # Run the Flask app. Cloud Run uses Gunicorn by default.
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
