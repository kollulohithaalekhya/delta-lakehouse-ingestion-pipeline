from flask_cors import CORS
from flask import Flask, request, jsonify
import duckdb

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

DB_PATH = "data/lakehouse"


def get_connection():
    con = duckdb.connect()
    con.execute("LOAD delta")
    return con


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/tables", methods=["GET"])
def list_tables():
    tables = [
        "bronze",
        "silver",
        "silver_corrected"
    ]
    return jsonify(tables)


@app.route("/api/query", methods=["POST"])
def run_query():
    query = request.json.get("query")

    try:
        con = get_connection()
        result = con.execute(query).fetchall()
        return jsonify({"data": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/preview/<table>", methods=["GET"])
def preview_table(table):
    try:
        con = get_connection()

        path = f"{DB_PATH}/{table}"

        result = con.execute(f"""
        SELECT * FROM delta_scan('{path}')
        LIMIT 10
        """).fetchall()

        return jsonify({"data": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)