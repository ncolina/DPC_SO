import imports
import os

print 'What would you like to do \n 1. Update database \n 2. Output CRM \n 3. Output database 4. Create database (BIGBANG)
response = raw_input("default[1]: ")
if response == 1 or response = False:
    newest = min(glob.iglob('database_backups/*.hdf5'), key=os.path.getctime)
    dbfile=raw_input('What database would you like to update? Input relative path. (default: %s) ' % newest)
    update=raw_input('What file will you use to update the database? Input relative path: ')
    if file == False:
        database=get_database(newest)
    else:
        database=get_database(dbfile)
    database=update_database(update,database)
    save_database(database)
elif response == 2:
    dbfile=raw_input('What database would you like to use to build the CRM? Input relative path. (default: %s) ' % newest)
    if file == False:
        database=get_database(newest)
    else:
        database=get_database(dbfile)
    create_crm(database)
