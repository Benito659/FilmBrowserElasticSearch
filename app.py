from flask import Flask, request, render_template,make_response
from elasticsearch import Elasticsearch
from flask_cors import CORS
from ingestion import ingestionElastic
es = Elasticsearch("https://es03:9200", ca_certs="http_ca.crt", basic_auth=("elastic", "Kkv8Pxw7wFYgtkt7fx02"))

MAX_SIZE = 15

app = Flask(__name__,template_folder='template')
CORS(app, resources={r"/*": {"origins": "*", "headers": ["Content-Type", "Authorization"]}})


@app.route("/")
def home():
    return render_template("index.html")

@app.route("/ingestion")
def ingestion():
    ingestionElastic()
    return render_template("index.html")

@app.route("/search")
def search_autocomplete():
    query = request.args["q"].lower()
    tokens = query.split(" ")
    print("tokens == ",tokens)
    clauses = [
        {
            "span_multi": {
                "match": {"fuzzy": {"title": {"value": i, "fuzziness": "AUTO"}}}
            }
        }
        for i in tokens
    ]

    payload = {
        "bool": {
            "must": [{"span_near": {"clauses": clauses, "slop": 3, "in_order": False}}]
        }
    }
    print("payload ==",payload)
    resp = es.search(index="film", query=payload, size=MAX_SIZE)
    print("json == ",resp)
    data = [result['_source']['title'] for result in resp['hits']['hits']]
    response = make_response(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0')