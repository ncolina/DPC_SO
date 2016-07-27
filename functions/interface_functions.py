from functions.imports import *
import os
import sys, argparse
import logging

def update_prompt():
    newest = get_newest_db()
    while True:
        dbfile=raw_input('What database would you like to update? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            print 'That file does not exist.'
            break
    while True:
        update=raw_input('What file will you use to update the database? Input relative path: ')
        if os.path.isfile(update) == True:
            print 'That file does not exist.'
            break
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
    yellowpages=raw_input('YellowPages? Y/N [Default:N] ') or 'N'
    ab_choice=raw_input('Would you like to substitue in abbreviations? Y/N [Default Y]: ') or 'Y'
    database=get_database(dbfile)
    if ab_choice == 'Y' or ab_choice =='y': #if abbreviations are not desired then it should be said by having any othe input except Y/y
        abbr=True
    else:
        abbr = False
    if yellowpages == 'N' or yellowpages =='N':
        choice=raw_input('What account type do you want? ALL, GO, BR or RR [Default ALL]:') or 'ALL'
        if choice == 'ALL':
            create_crm_csv(database)
        elif choice == 'GO':
            create_government_crm(database, export=True,abbr=abbr)
        elif choice == 'BR':
            create_buisness_crm(database, export=True,abbr=abbr)
        elif choice == 'RR':
            create_residential_crm(database, export=True,abbr=abbr)
        else:
            print "Invalid input!"
    elif yellowpages=='y' or yellowpages=='Y':
        create_yellowpages_crm(database)


def export_prompt():
    newest = get_newest_db()

    while True:
        dbfile=raw_input('What database would you like to export? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    database=get_database(dbfile)
    database2csv(database)

def big_bang():
    while True:
        update=raw_input('What file will you use to create the database? Input relative path: ')
        if os.path.isfile(update) == True:
            break
        print 'That file does not exist.'
    database=update_database(update,None,bigbang=True)
    save_database(database)

def interactive():
    while True:
        print 'What would you like to do?\n1. Update database \n2. Export CRM \n3. Export database \n4. Create database (BIGBANG!)\n5. Exit'
        response = raw_input("default[1]: ") or '1'
    # Updating the database
        if response == '1':
            update_prompt()
    #Exporting in CRM format with excel file
        elif response == '2':
            crm_prompt()
    #Exporting database to excel file
        elif response == '3':
            export_prompt()
    #Bigbang: creating the databse from scatch
        elif response == '4':
            big_bang()
    #Exit
        elif response =='5':
            break
        else:
            print 'Invalid input!'

def commandline():
    parser = argparse.ArgumentParser(description='Get what to do from command line switches')
    parser.add_argument('-i', '--inputfile',dest='input',help='inputfile to update/create the db')
    parser.add_argument('-db', '--database',dest='db',help='databse to be exported or updated')
    parser.add_argument('-o', '--outputfile',dest='output',help='output filename/location')
    parser.add_argument('-m', '--mode', dest='mode',default='interactive',choices=['interactive','update_db', 'bigbang', 'db2xls','db2csv', 'exportcrm'], help='update_db bigbang exportdb exportcrm exportgo exportbr exportrr')
    args = parser.parse_args()
    if args.mode=='update_db':
        dbfile=args.db
        output=args.output
        inputfile=args.input
        if os.path.isfile(dbfile) == False:
            sys.exit("database file does not exist")
        if os.path.isfile(inputfile) == False:
            sys.exit("input file does not exist")
        database=get_database(dbfile)
        database=update_database(inputfile,database)
        save_database(database,filename=output)
    elif args.mode=='bigbang':
        update=args.input
        output=args.output
        if os.path.isfile(update) == False:
            sys.exit("input file does not exist")
        database=update_database(update,None,bigbang=True)
        save_database(database,filename=output)
    elif args.mode=='db2xls':
        dbfile=args.db
        output=args.output
        if os.path.isfile(dbfile) == False:
            sys.exit("database file does not exist")
        database=get_database(dbfile)
        database2xls(database,filename=output)
    elif args.mode=='db2csv':
        dbfile=args.db
        output=args.output
        if os.path.isfile(dbfile) == False:
            sys.exit("database file does not exist")
        database=get_database(dbfile)
        database2csv(database,filename=output)
    elif args.mode=='exportcrm':
        dbfile=args.db
        output=args.output
        if os.path.isfile(dbfile) == False:
            sys.exit("database file does not exist")
        database=get_database(dbfile)
        create_crm(database,filename=output)
    elif args.mode=='interactive':
        interactive()
