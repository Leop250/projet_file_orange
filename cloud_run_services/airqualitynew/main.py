from flask import Flask, jsonify
import sys
import os

# Ajouter le chemin parent pour importer les modules centralisés
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from extract.extract_airquality import extract_air_quality_openmeteo
from transform.transform_airquality import transform_airquality_data
from load.load_airquality import load_airquality_to_bigquery

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "service": "Air Quality API"}), 200

@app.route("/fetch", methods=["POST", "GET"])
def fetch_air_quality():
    try:
        # EXTRACT
        df_raw = extract_air_quality_openmeteo()
        
        # TRANSFORM
        df_transformed = transform_airquality_data(df_raw)
        
        # LOAD
        rows_loaded = load_airquality_to_bigquery(df_transformed)
        
        return jsonify({"status": "success", "rows": rows_loaded}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
