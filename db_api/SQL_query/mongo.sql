--To get the all data without any filter condition
results=collection.find({})
for data in results:
    if 'first_name' in data.keys():
        print(data['first_name'])
    else:
        print('first_name key is not available \n')

--To get the data where _id == Test001
query1 = {"_id":"Test001"}
results=collection.find(query1)
for data in results:
    print(data['first_name'])

--To get the data where dob between 1930-01-01 and 2000-01-01
query2 = {"dob":{"$gte":"1930-01-01",
                "$lt":"2000-01-01"}}

results=collection.find(query2)
for data in results:
    print(data['first_name'])

--To get the data where dob between 1930-01-01 and 2000-01-01
--and  marital_status==Unmarried
query3 = {"dob":{"$gte":"1930-01-01",
                "$lt":"2000-01-01"},
        "marital_status":"Unmarried"
                }

results=collection.find(query3)
for data in results:
    print(data['first_name'],data['marital_status'])

--To get the data where dob between 1930-01-01 and 2000-01-01
--and limit the data by 5 (same as top 5 in mssql)
query4 = {"dob":{"$gte":"1930-01-01",
                "$lt":"2000-01-01"}
                }

results=collection.find(query4).limit(5)
for data in results:
    print(data['first_name'],data['marital_status'])

--To get the unique/distinct first_name from collection
results=collection.distinct('first_name')
for data in results:
    print(data)

--To get the sum/count of all data based on marital_status
results=collection.aggregate([{"$group":{"_id":"$marital_status","num":{"$sum":1}}}])
for data in results:
    print(data)


--To get the data where middle_name column is available and which have some data
query5 = {"middle_name":{"$exists":True,
                        "$ne":""}
                }

results=collection.find(query5)
for data in results:
    print(data['middle_name'])
    print(data['contact_point']['postal_address']['Country'])
    print(data['contact_point']['email_address'])
    print(data['contact_point']['telephone']['mobile'])

--To get the duplicate values which appears more than 1 times
results=collection.aggregate([
                            {"$group":{"_id":"$first_name","count":{"$sum":1}}},
                            {"$match":{"_id":{"$ne": ""},"count":{"$gt":1}}},
                            {"$project":{"name":"$_id","_id":0}}
                            ])
for data in results:
    print(data)

--To get the data where last_name holds the Mish as text (same as like operator in mssql)
query6 = {"middle_name":{"$exists":True,
                        "$ne":""}
                }

results=collection.find({"last_name":{"$regex":".*Mish*."}})
for data in results:
    print(data)

--To get the data where dob between 1930-01-01 and 2000-01-01
--and unique/distinct value of first_name
query7 = {"dob":{"$gte":"1930-01-01",
                "$lt":"2000-01-01"}
                }

results=collection.distinct('first_name',query7)
for data in results:
    print(data)