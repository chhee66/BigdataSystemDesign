from flask import Flask, render_template, redirect, request, url_for
from pymongo import MongoClient
import pprint

app = Flask(__name__)

@app.route('/')
@app.route('/mongo', methods=['POST'])
def mongodb():
    client = MongoClient('mongodb://localhost:27017/')#defalut host
    db = client.flights #db name
    collec = db.myCollection#collection name
    
    
    #1번 쿼리w : 비행시간;이 1.5이상 거리에서의 좌석 인기 순위-------------------------------------------------------------------------------------------------
    pipeline = [{'$match' : {"time": {"$gte" : 1.5}}}, {'$group':{'_id':"$flightType", 'popular':{'$sum':1}}}, {'$sort':{'popular': -1}}]
    # pipeline = [{'$group':{'_id':"$flightType", 'popular':{'$sum':1}}}, {'$sort':{'totalTime':-1}}]
    results1=collec.aggregate(pipeline)
    
    
    #2번 쿼리: 항공사별 월간 운항 횟수 -----------------------------------------------------------------------------------------------
    years = ["2019", "2020", "2021", "2022", "2023"]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    results2 = []

    for year in years :
        for month in months :
            pipeline = [{'$match' : {"date": {"$regex" : month + "/[0-9]{2}\/" + year }}}, {"$group":{"_id":"$agency", "count":{"$sum":1}}}, {'$sort':{'_id': 1}}]

            count = 0
            test = list(collec.aggregate(pipeline))

            for i in test :
                count = count + i['count']

            if count==0 :
                continue

            tmp = [year + '-' + month, test]

            results2.append(tmp)
    
    
    #3번 쿼리: 항공사 별 총 비행거리----------------------------------------------------------------------------
    pipeline = [{"$group":{"_id":"$agency", "dist":{"$sum":"$distance"}}}]
    
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
    results5=collec.aggregate(pipeline)



    #6번 쿼리: agency별 flightType 이용 횟수--------------------------------------------------------------------
    pipeline = [{'$group':{'_id':'$agency'}}, {'$sort':{'_id': 1}}]
    agency = list(collec.aggregate(pipeline)) # [{'_id': 'FlyingDrops'}, {'_id': 'CloudFy'}, {'_id': 'Rainbow'}]
    agencies = []
    results6 = []

    for i in range(len(agency)):
        agencies.append(agency[i]['_id']) # ['FlyingDrops', 'CloudFy', 'Rainbow']

    for j in agencies:
        pipeline = [{'$match':{'agency':j}}, {'$group':{'_id':'$flightType', 'count':{'$sum':1}}}, {'$sort':{'_id': 1}}]
        count = list(collec.aggregate(pipeline))
        tmp = [j, count]

        results6.append(tmp)


    # ----------------------------------------------------------------------------------------------------------
    client.close()
    #rendering
    return render_template('mongo.html', data1=list(results1), data2=results2, data3=list(results3), data4=results4, data5=list(results5), data6=results6)
    
if __name__ == '__main__':
    app.run(debug=True)