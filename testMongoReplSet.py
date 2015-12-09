#!/usr/bin/python
# Script to connect to MongoDB though Python.
#
# Author : Ben Rich

from pymongo import MongoClient
from pymongo import ReadPreference
from time import sleep

count = 0
mongouri = "mongodb://app:password@127.0.0.1:27017,10.116.2.166:27017,10.116.2.167:27018/test?replicaSet=test"
client = MongoClient(mongouri, 
                     connectTimeoutMS=10000,
                     socketTimeoutMS=None,
                     socketKeepAlive=True); print client; sleep(0.1); print client
db = client.get_database('test', read_preference=ReadPreference.NEAREST)


print ( "Displaying one  Registration")
print (db.Registration.find_one())  # Display one record from database (db), collection ( subscriptions)

print ( "Printing count of records in Registration collection")
print (db.Registration.count())  # Count records in Registration collection

print ( "Printing count of records in settings collection")
print (db.settings.count()) # count records in settings collection
assert(db.settings.count() == 35)


