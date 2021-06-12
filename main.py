from flask import Flask, render_template, redirect, request, url_for
from pymongo import MongoClient
import pprint

app = Flask(__name__)

@app.route('/')
#쿼리 3번 
@app.route('/mongo', methods=['POST'])
def mongodb():
    client = MongoClient('mongodb://localhost:27017/')#defalut host
    db = client.flights#db name
    collec = db.myCollection#collection name
    #1번 쿼리-------------------------------------------------------------------------------------------------
    #2번 쿼리-------------------------------------------------------------------------------------------------
    #3번 쿼리: 항공사 별 총 비행거리----------------------------------------------------------------------------
    pipeline = [{"$group":{"_id":"$agency", "dist":{"$sum":1}}}]
    results3 = collec.aggregate(pipeline)

    #4번 쿼리:도착지(to)별 이용 횟수(기준:userCode)-------------------------------------------------------------
    pipeline = [{"$group":{"_id":"", "destination":{"$addToSet":"$to"}}}, {"$project":{"_id":0}}]
    destination = list(collec.aggregate(pipeline))[0]['destination'] #['Campo Grande (MS)', 'Sao Paulo (SP)', 'Recife (PE)', 'Salvador (BH)', 'Aracaju (SE)', 'Brasilia (DF)', 'Natal (RN)', 'Florianopolis (SC)', 'Rio de Janeiro (RJ)']
    counts = []
    for i in range(len(destination)): # 0~8
        pipeline = [{"$match":{"to":{"$eq":destination[i]}}}, {"$group":{"_id":"userCode", "count":{"$sum":1}}}] # [{'_id': 'userCode', 'count': 34748}]
        counts.append(list(collec.aggregate(pipeline))[0]['count']) # [{'_id': 'userCode', 'count': 34748}]
    #print(counts) #[34748, 23625, 30480, 17104, 37224, 30779, 23796, 57317, 16815] # 인덱스=destination
    key_value = [destination, counts]
    results4 = dict(zip(*key_value)) # key:destination, value:counts
    #print(results4) # {'Campo Grande (MS)': 34748, 'Sao Paulo (SP)': 23625, 'Recife (PE)': 30480, 'Salvador (BH)': 17104, 'Aracaju (SE)': 37224, 'Brasilia (DF)': 30779, 'Natal (RN)': 23796, 'Florianopolis (SC)': 57317, 'Rio de Janeiro (RJ)': 16815}
    #5번 쿼리: 가장 비행시간이 긴 승객 TOP5---------------------------------------------------------------------
    pipeline = [{'$group':{'_id':"$userCode", 'totalTime':{'$sum':"$time"}}}, {'$sort':{'totalTime':-1}}, {'$limit':5}]
    result5=collec.aggregate(pipeline)

    #----------------------------------------------------------------------------------------------------------
    client.close()
    #rendering
    return render_template('mongo.html', data3=results3, data4=results4, data5=result5)
    
if __name__ == '__main__':
    app.run(debug=True)