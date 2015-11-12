from pymongo import MongoClient, ASCENDING
import subprocess
import argparse
import sys
from datetime import datetime


def connect_to_db(dbserver, db, port, admin_password, auth_db=None):
    server = dbserver
    password = admin_password
    try:
        client = MongoClient(server, port)
        database = client[db]
    except:
        print "failed to connect to mongodb://%s:%s/%s" % (server, port, db)
        sys.exit(1)
    if password != None:
        try:
            database.authenticate('admin', password, source=auth_db)
        except:
            print "Authentication failure when attempting to database mongodb://%s:%s/%s" % (server, port, db)
            sys.exit(1)
    return database


def process_command_line():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--dbserver',
                        action='store',
                        dest='dbserver',
                        default=None,
                        help="hostname of a mongos server"
                        )
    options = parser.parse_args()
    return options


def main():
    options = process_command_line()
    # finding the dbserver
    if options.dbserver:
        dbserver = options.dbserver

    db = "test"
    print str(datetime.now()) + ' Connecting to %s on %s' % (db, dbserver)
    database = connect_to_db(dbserver, db, 27017, auth_db="admin")

    # checking to see if  index needs to be dropped
    indexes = database.lists.index_information()
    if 'listId_1' in indexes.keys():
        listId_index_exists = True
    else:
        listId_index_exists = False
    # dropping and recreating index for sharding
    print str(datetime.now()) + ' The unique listId index exists for collection lists =' + ' ' + str(listId_index_exists)
    if listId_index_exists:
        print str(datetime.now()) + ' Dropping listId_1 index from lists collection'
        database.lists.drop_index('listId_1')
        # delete records from the lists collections
        print str(datetime.now()) + ' Removing documents from lists collection'
        database.lists.remove({})
        print str(datetime.now()) + ' Creating list shardkey index'
        database.lists.create_index([("region", ASCENDING),
                                         ("random", ASCENDING),
                                         ("listId", ASCENDING)])

    # checking to see if list_events listId index needs to be dropped
    indexes = database.list_events.index_information()
    if 'listId_1' in indexes.keys():
        list_events_listId_index_exists = True
    else:
        list_events_listId_index_exists = False
    # dropping and recreating index for sharding
    print str(datetime.now()) + ' The unique listId index exists for collection list_events =' + ' ' + str(
        list_events_listId_index_exists)
    if list_events_listId_index_exists:
        print str(datetime.now()) + ' Dropping listId_1 index form list_events collection'
        database.list_events.drop_index('listId_1')
        # delete records from the list_events collection
        print str(datetime.now()) + ' Removing documents from list_events collection'
        database.list_events.remove({})
        print str(datetime.now()) + ' Creating list_events shardkey index'
        database.list_events.create_index([("region", ASCENDING),
                                               ("random", ASCENDING),
                                               ("listId", ASCENDING)])

    # checking to see if list_names listId index needs to be dropped
    indexes = database.list_names.index_information()
    if 'listId_1' in indexes.keys():
        list_names_listId_index_exists = True
    else:
        list_names_listId_index_exists = False
    # dropping and recreating index for sharding
    print str(datetime.now()) + ' The unique listId index exists for list_names =' + ' ' + str(list_names_listId_index_exists)
    if list_names_listId_index_exists:
        print str(datetime.now()) + ' Dropping listId_1 index from list_names collection'
        database.list_names.drop_index('listId_1')
        # delete records from the list_names collection
        print str(datetime.now()) + ' Removing documents from list_names collection'
        database.list_names.remove({})
        print str(datetime.now()) + ' Creating list_names shardkey index'
        database.list_names.create_index([("region", ASCENDING),
                                            ("random", ASCENDING),
                                            ("listId", ASCENDING)])

    # checking to see if psn_lock listId index needs to be dropped
    indexes = database.psn_lock.index_information()
    if 'region_1_listId_1' in indexes.keys():
       psn_lock_listId_index_exists = False
    else:
        psn_lock_listId_index_exists = True
    # dropping and recreating index for sharding
    print str(datetime.now()) + ' The unique listId index exists for psn_lock =' + ' ' + str(psn_lock_listId_index_exists)
    if psn_lock_listId_index_exists:
        #print str(datetime.now()) + ' Dropping listId_1 index from psn_lock collection'
        #database.list_names.drop_index('listId_1')
        # delete records from the list_names collection
        print str(datetime.now()) + ' Removing documents from psn_lock collection'
        database.psn_lock.remove({})
        print str(datetime.now()) + ' Creating psn_lock shardkey index'
        database.psn_lock.create_index([("region", ASCENDING),
                                        ("listId", ASCENDING)])

    # connecting to the admin DB
    db = "admin"
    print str(datetime.now()) + ' Connecting to %s on %s' % (db, dbserver)
    database = connect_to_db(dbserver, db, 27017, auth_db="admin")

    # shard the lists collection
    try:
        database.command('shardcollection', 'test.lists', key={'region': 1, 'random': 1, 'listId': 1},
                         unique=True)
    except:
        print str(datetime.now()) + ' The lists collection has already been sharded'

    # shard the list_events collection
    try:
        database.command('shardcollection', 'test.list_events', key={'region': 1, 'random': 1, 'listId': 1}, unique=True)
    except:
        print str(datetime.now()) + ' The list_events collection has already been sharded'

    # shard the list_names collection
    try:
        database.command('shardcollection', 'test.list_names', key={'region': 1, 'random': 1, 'listId': 1}, unique=True)
    except:
        print str(datetime.now()) + ' The list_names collection has already been sharded'

    # shard the psn_lock collection
    try:
        database.command('shardcollection', 'test.psn_lock', key={'region': 1, 'listId': 1}, unique=True)
    except:
        print str(datetime.now()) + ' The psn_lock collection has already been sharded'


if __name__ == '__main__':
    main()
