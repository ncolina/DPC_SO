from functions.imports import *
import os

def update_prompt():
    newest = get_newest_db()
    while True:
        dbfile=raw_input('What database would you like to update? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    while True:
        update=raw_input('What file will you use to update the database? Input relative path: ')
        if os.path.isfile(update) == True:
            break
        print 'That file does not exist.'
    database=get_database(dbfile)
    database=update_database(update,database)
    save_database(database)

def crm_prompt():
    newest = get_newest_db()
    while True:
        dbfile=raw_input('What database would you like to use to build the CRM? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    choice=raw_input('What account type do you want? ALL, GO, BR or RR [Default ALL]:') or 'ALL'
    database=get_database(dbfile)
    if choice == 'ALL':
        create_crm(database)
    elif choice == 'GO':
        create_government_crm(database, export=True)
    elif choice == 'BR':
        create_buisness_crm(database, export=True)
    elif choice == 'RR':
        create_residential_crm(database, export=True)
    else:
        print "Invalid input!"

def export_prompt():
    newest = get_newest_db()

    while True:
        dbfile=raw_input('What database would you like to export? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    database=get_database(dbfile)
    database2xls(database)

def big_bang():
    while True:
        update=raw_input('What file will you use to create the database? Input relative path: ')
        if os.path.isfile(update) == True:
            break
        print 'That file does not exist.'
    database=update_database(update,None,bigbang=True)
    save_database(database)
