from pymongo import MongoClient


def connect_to_db(dbserver, dbport, dbuser, dbpassword, dbname):
    """return a connection to mongodb"""
    try:
        client = MongoClient(dbserver, dbport)
        database = client[dbname]
        database.authenticate(dbuser, dbpassword, source='admin')
        return database
    except Exception as e:
        print type(e), e


def indexes_for_collection(dbconnection, collection):
    """return a cursor of connections for dbconnection, collection"""
    mycol = dbconnection[collection]
    return mycol.list_indexes()



def list_collections(dbconnection):
    """return a list of collections for dbconnection"""
    try:
        return dbconnection.collection_names()
    except Exception as e:
        print type(e), e
