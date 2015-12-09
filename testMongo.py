#!/usr/bin/python
# Script to connect to MongoDB though Python.
#
# Author : Ben Rich

from pymongo import MongoClient
from pymongo import ReadPreference

count = 0
# MongoDB  connection string
uri = 'mongodb://admin:administrator@192.168.100.25:27017/test'
client = MongoClient(uri)
db = client['admng']

# Assert db name
assert db.name == 'test'

# Check the link for various ReadPreference options
# http://api.mongodb.org/python/current/api/pymongo/
db.read_preference = ReadPreference.PRIMARY  # Important, must be provided

print ( "Printing server information")
print (client.server_info())  # Print Server information

print ( " Printing collection names")
print (db.collection_names())  # Print names of the all the collections

print ( " Verify collection names exist")
assert db.collection_names().__contains__("collection1")
assert db.collection_names().__contains__("collection2")
assert db.collection_names().__contains__("collection3")

print ( "Printing database names")
print (client.database_names())  

print ( "Displaying one Ads Registration")
print (db.collection1.find_one())  # Display one record from database (db), collection ( subscriptions)

print ( "Printing count of records in AdsRegistration table")
print (db.collection1.count())  # Count records in collection1
subs_col = db.collection1



