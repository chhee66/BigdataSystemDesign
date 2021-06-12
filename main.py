from flask import Flask, render_template, redirect, request, url_for
from pymongo import MongoClient
import pprint

app = Flask(__name__)

@app.route('/')
@app.route('/mongo', methods=['POST'])
def mongoTest():
    client = MongoClient('mongodb://localhost:27017/')#defalut host
    db = client.flights#db name
    collec = db.myCollection#collection name

    pipeline = [{"$group":{"_id":"$agency", "dist":{"$sum":1}}}]
    results = collec.aggregate(pipeline)

    client.close()

    return render_template('mongo.html', data=results)

if __name__ == '__main__':
    app.run(debug=True)