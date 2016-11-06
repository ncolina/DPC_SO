from functions.imports import *
import os
import sys, argparse
import logging
import time

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
    name='database_backups/database-%s.hdf5'% time.strftime('%Y-%m-%d-%H-%M-%S')
    filename=raw_input('What would you like to name the updated database? [Default: %s]'%name) or name
    database=get_database(dbfile)
    database=update_database(update,database)
    save_database(database,filename)
    logging.info('%s has been updated and saves as %s',dbfile,filename)


def crm_prompt():
    newest = get_newest_db()
    while True:
        dbfile=raw_input('What database would you like to use to build the CRM? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    yellowpages=raw_input('YellowPages? Y/N [Default:N] ') or 'N'
    cap_choice=raw_input('Should capitalization of names and streets be fixed? Y/N [Default Y]: ')or 'Y'
    ab_choice=raw_input('Would you like to substitue in abbreviations? Y/N [Default Y]: ') or 'Y'
    name_sub_choice=raw_input('Should company names be expanded? Y/N [Default Y]: ') or 'Y'
    mult_or=raw_input('"Multiple or"/or "single or". M/S [Default S]: ') or "S"
    name='crm-%s.csv'% time.strftime('%Y-%m-%d-%H-%M-%S')
    filename=raw_input('What would you like the base name for the exported crm to be? [Default: %s]'%name) or name
    database=get_database(dbfile)
    if cap_choice=="Y" or cap_choice=='y':
        cap=True
    else:
        cap=False
    if mult_or=="M" or mult_or=='m':
        multi_or=True
    else:
        multi_or=False
    if ab_choice == 'Y' or ab_choice =='y': #if abbreviations are not desired then it should be said by having any othe input except Y/y
        abbr=True
    else:
        abbr = False
    if name_sub_choice == 'Y' or name_sub_choice =='y': #if expansion is not desired then it should be said by having any othe input except Y/y
        name_sub=True
    else:
        name_sub = False
    if yellowpages=='y' or yellowpages=='Y':
        create_yellowpages_crm(database, filename=filename, abbr=abbr,multi_or=multi_or,name_sub=name_sub,cap=cap)
    else:# yellowpages == 'N' or yellowpages =='n':
        choice=raw_input('What account type do you want? ALL, GO, BR or RR [Default ALL]:') or 'ALL'
        if choice == 'ALL':
            create_crm_csv(database, filename=filename, abbr=abbr,multi_or=multi_or , name_sub=name_sub,cap=cap)
        elif choice == 'GO':
            create_government_crm(database, filename=filename, export=True,abbr=abbr,multi_or=multi_or, name_sub=name_sub,cap=cap)
        elif choice == 'BR':
            create_buisness_crm(database, filename=filename, export=True,abbr=abbr,multi_or=multi_or, name_sub=name_sub, cap=cap)
        elif choice == 'RR':
            create_residential_crm(database, filename=filename, export=True,abbr=abbr,multi_or=multi_or, cap=cap)
        else:
            print "Invalid input!"


def export_prompt():
    newest = get_newest_db()

    while True:
        dbfile=raw_input('What database would you like to export? Input relative path. (default: %s) ' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    name='db-%s.csv'% time.strftime('%Y-%m-%d-%H-%M-%S')
    filename=raw_input('What would you like the base name for the exported database to be? [Default: %s]'%name) or name
    database=get_database(dbfile)

    database2csv(database,filename)
    logging.info('Database has been exported as a csv from %s', dbfile)


def big_bang():
    while True:
        update=raw_input('What file will you use to create the database? Input relative path: ')
        if os.path.isfile(update) == True:
            break
        print 'That file does not exist.'
    name='database_backups/database-%s.hdf5'% time.strftime('%Y-%m-%d-%H-%M-%S')
    filename=raw_input('What would you like to name the database? [Default: %s]'%name) or name
    database=update_database(update,None,bigbang=True)
    save_database(database,filename)
    logging.info('Database has been created from %s  ', update)

def similar():
    newest = get_newest_db()

    while True:
        dbfile=raw_input('What database will be looked at for similar names? Input relative path: [Default: %s]' % newest) or newest
        if os.path.isfile(dbfile) == True:
            break
        print 'That file does not exist.'
    probability=float(raw_input('Similarity probability [Default: 0.6]')or 0.6)
    name='similar_%s'%time.strftime('%Y-%m-%d-%H-%M-%S')
    filename=raw_input('What would you like to name the file containing the similar names? [Default: %s]'%name) or name

    database=get_database(dbfile)
    database=database.drop(['src','class_code','user','so_rangedate'],1)
    similar_names=find_similar_names(database,probability=probability)
    if len(similar_names)>0:
        to_fwf(similar_names,filename)
        logging.info('Similar entries have been saved in the input file format as %s',filename)
    else:
        logging.info('There are no sililar names.')

def interactive():
    while True:
        print 'What would you like to do?\n1. Update database \n2. Export CRM \n3. Export database \n4. Create database (BIGBANG!)\n5. Find similar names.\n6. Exit'
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
        elif response =='5':
            similar()
    #Exit
        elif response =='6':
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
    elif args.mode=='similar':
        dbfile=args.db
        output=args.output
        if os.path.isfile(dbfile) == False:
            sys.exit("database file does not exist")
        database=get_database(dbfile)
        database=find_similar_names(database)
        to_fwf(database,filename=output)
    elif args.mode=='interactive':
        interactive()
