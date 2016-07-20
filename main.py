from functions.interface_functions import *
import os
import glob


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
