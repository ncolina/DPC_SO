import pandas as pd
import time
import numpy as np
from functions.capitalization import titlecase
import csv
import glob
import os
import ConfigParser
import logging

<<<<<<< HEAD
=======
Config = ConfigParser.ConfigParser()
Config.read("config/input_format")
Config.read("config/areacodes")
Config.read("config/prov_abbreviations")
Config.read("config/city_abbreviations")
prov_abbr=ConfigSectionMap('prov_abbreviations')
city_abbr=ConfigSectionMap('city_abbreviations')
codes=ConfigSectionMap('areacodes')

>>>>>>> origin/master
def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

<<<<<<< HEAD

Config = ConfigParser.ConfigParser()
Config.read("config/input_format")
Config.read("config/areacodes")
Config.read("config/prov_abbreviations")
Config.read("config/city_abbreviations")
prov_abbr=ConfigSectionMap('prov_abbreviations')
city_abbr=ConfigSectionMap('city_abbreviations')
codes=ConfigSectionMap('areacodes')


=======
>>>>>>> origin/master
def get_database(file):
    database=pd.read_hdf(file,'database')
    return  database

def update_database(update_file,database,bigbang=False):
    so_file=update_file
    names = Config.options('input_format')
    widths=[int(Config.get('input_format',name)) for name in names ]
    update=pd.read_fwf(so_file,
                   header=None,
                   widths=widths,
                   names=names,
                   converters = {'mem_wstd': str, 'sam_stnmfr':str,'account_no':str,'old_wstd':str,'so_date':str},
                   #error_bad_lines=True,
                   #dtype='object',
                   index_col=None
                  )
<<<<<<< HEAD
    update.fillna('', inplace=True)
=======
    #update.fillna('', inplace=True)
>>>>>>> origin/master

    update['acc_type']=update['acc_type'].astype('category')
    update['src']=so_file.split('/')[-1]
    update['so_rangedate']=time.strftime("%Y-%m-%d")
    update['user']= os.getlogin()
    if bigbang == True:
        update.sort_values(by='last_name',inplace=True)
        update.drop_duplicates(names,inplace=True)
        update.reset_index(drop=True,inplace=True)
        return update
    database.append(update)
    database.sort_values(by='last_name',inplace=True)
    database.drop_duplicates(names,inplace=True)
    database.reset_index(drop=True,inplace=True)
    return database

def save_database(database,filename=None):
    filename=filename or 'database_backups/database-%s.hdf5'% time.strftime('%Y-%m-%d-%H-%M-%S')
    database.to_hdf(filename, 'database', mode='w', format='table')
    print "Database has been saved as %s in the database_backups folder"% filename

#Input database as a  pandas dataframe and returns a pandas dataframe with data in CRM format
#Specify export = True if an excel export is required, in this case the function returns nothing
def create_residential_crm(database,export=False,filename=None,abbr=True):
    rr=database[database.acc_type=='RR']
    rr=rr[rr.list_code=='PB']

    rr_crm=pd.DataFrame()
    rr_crm['Areacode']=get_areacode(rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    rr_crm['Phone']=rr.mem_wstd.str.slice(-7)
    rr_crm['name1']=rr.last_name
    rr_crm['name2']=rr.first_name
    rr_crm['SAM_BLDNAME']=rr.sam_bldname
    rr_crm['SAM_STNMFR']=rr.sam_stnmfr
    rr_crm['SAM_STNAME']=rr.sam_stname
    rr_crm['SAM_STSUBT']=rr.sam_stsubt
    rr_crm['sam_estate']=rr.sam_estate
    rr_crm['City']=rr.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip())
    rr_crm['Province']=rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip())
    rr_crm['class_code']=None
    rr_crm['class_desc']=None
    rr_crm.name1 = rr_crm.name1.apply(lambda x: titlecase(x))
    rr_crm.name2 = rr_crm.name2.apply(lambda x: titlecase(x))
    rr_crm.SAM_BLDNAME = rr_crm.SAM_BLDNAME.apply(lambda x: titlecase(x))
    rr_crm.SAM_STNAME = rr_crm.SAM_STNAME.apply(lambda x: titlecase(x))
    rr_crm.SAM_STSUBT = rr_crm.SAM_STSUBT.apply(lambda x: titlecase(x))
    rr_crm.sam_estate = rr_crm.sam_estate.apply(lambda x: titlecase(x))
    rr_crm.City = rr_crm.City.apply(lambda x: titlecase(x))
    rr_crm.Province = rr_crm.Province.apply(lambda x: titlecase(x))
    fix_duplicate(rr_crm)

    #or call may not be necessary here
    rr_crm= or_call(rr_crm)
    if abbr == True:
        apply_abbr(rr_crm)
    if export == True:
        filename=filename or 'crm_rr_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        rr_crm.to_csv(filename)
        #writer.save()
        print 'RR CRM saved in csv format with file name %s'%filename
    return rr_crm

def create_government_crm(database,export=False,filename=None,abbr=True):
    go=database[database.acc_type=='GO']
    go=go[go.list_code=='PB']

    go_crm=pd.DataFrame()
    go_crm['Areacode']=get_areacode(go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    go_crm['Phone']=go.mem_wstd.str.slice(-7)
    go_crm['name1']=go.last_name
    go_crm['name2']=go.first_name
    go_crm['SAM_BLDNAME']=go.sam_bldname
    go_crm['SAM_STNMFR']=go.sam_stnmfr
    go_crm['SAM_STNAME']=go.sam_stname
    go_crm['SAM_STSUBT']=go.sam_stsubt
    go_crm['sam_estate']=go.sam_estate
    go_crm['City']=go.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip())
    go_crm['Province']=go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip())
    go_crm['class_code']=""
    go_crm['class_desc']=""
    go_crm.name1 = go_crm.name1.apply(lambda x: titlecase(x))
    go_crm.name2 = go_crm.name2.apply(lambda x: titlecase(x))
    go_crm.SAM_BLDNAME = go_crm.SAM_BLDNAME.apply(lambda x: titlecase(x))
    go_crm.SAM_STNAME = go_crm.SAM_STNAME.apply(lambda x: titlecase(x))
    go_crm.SAM_STSUBT = go_crm.SAM_STSUBT.apply(lambda x: titlecase(x))
    go_crm.sam_estate = go_crm.sam_estate.apply(lambda x: titlecase(x))
    go_crm.City = go_crm.City.apply(lambda x: titlecase(x))
    go_crm.Province = go_crm.Province.apply(lambda x: titlecase(x))
    fix_duplicate(go_crm)

    go_crm= or_call(go_crm)
    if abbr == True:
        apply_abbr(go_crm)
    if export == True:
        filename = filename or 'crm_go_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        go_crm.to_csv(filename)
        #writer.save()
        print 'GO CRM saved in csv format with file name %s'% filename
    return go_crm

def create_buisness_crm(database,export=False,filename=None,abbr=True):
    br=database[database.acc_type=='BR']
    br=br[br.list_code=='PB']
    br_crm=pd.DataFrame()
    br_crm['Areacode']=get_areacode(br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    br_crm['Phone']=br.mem_wstd.str.slice(-7)
    br_crm['name1']=br.last_name
    br_crm['name2']=br.first_name
    br_crm['SAM_BLDNAME']=br.sam_bldname
    br_crm['SAM_STNMFR']=br.sam_stnmfr
    br_crm['SAM_STNAME']=br.sam_stname
    br_crm['SAM_STSUBT']=br.sam_stsubt
    br_crm['sam_estate']=br.sam_estate
    br_crm['City']=br.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip())
    br_crm['Province']=br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip())
    br_crm['class_code']=""
    br_crm['class_desc']=""
    br_crm.name1 = br_crm.name1.apply(lambda x: titlecase(x))
    br_crm.name2 = br_crm.name2.apply(lambda x: titlecase(x))
    br_crm.SAM_BLDNAME = br_crm.SAM_BLDNAME.apply(lambda x: titlecase(x))
    br_crm.SAM_STNAME = br_crm.SAM_STNAME.apply(lambda x: titlecase(x))
    br_crm.SAM_STSUBT = br_crm.SAM_STSUBT.apply(lambda x: titlecase(x))
    br_crm.sam_estate = br_crm.sam_estate.apply(lambda x: titlecase(x))
    br_crm.City = br_crm.City.apply(lambda x: titlecase(x))
    br_crm.Province = br_crm.Province.apply(lambda x: titlecase(x))
    fix_duplicate(br_crm)
    #place orcall function here since we want LR and Lr to be duplicates
    br_crm= or_call(br_crm)
    if abbr == True:
        br_crm=apply_abbr(br_crm)
    if export == True:
        filename= filename or 'crm_br_%s.xlsx' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        br_crm.to_excel(filename)
        #writer.save()
        print 'BR CRM saved in csv format with file name %s'%filename
    return br_crm

def create_crm(database,filename=None):
    filename=filename or'crm_%s.xlsx' % time.strftime('%Y-%m-%d-%H-%M-%S')
    writer = pd.ExcelWriter(filename)
    create_residential_crm(database).to_excel(writer,'RR')
    create_government_crm(database).to_excel(writer,'GO')
    create_buisness_crm(database).to_excel(writer,'BR')
    writer.save()
    print 'CRM saved in xlsx format with file name %s'%filename

def create_crm_csv(database,filename=None):
    filename=filename or'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    create_residential_crm(database).to_csv('rr_%s'%filename)
    create_government_crm(database).to_csv('go_%s'%filename)
    create_buisness_crm(database).to_csv('br_%s'%filename)
    print 'CRM saved in csv format with file names %s'%filename

def create_yellowpages_crm(database, filename=None): # NOT DONE YET!!!!
    filename=filename or'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    create_residential_crm(database).to_csv('rr_%s'%filename)
    create_government_crm(database).to_csv('go_%s'%filename)
    create_buisness_crm(database).to_csv('br_%s'%filename)
    print 'Yellow Pages CRM saved in csv format with file names %s'%filename

def database2xls(database,filename=None):
    filename=filename or 'db_%s.xlsx' % time.strftime('%Y-%m-%d-%H-%M-%S')
    writer = pd.ExcelWriter(filename)
    database[database.acc_type=='RR'].to_excel(writer,'RR')
    database[database.acc_type=='GO'].to_excel(writer,'GO')
    database[database.acc_type=='BR'].to_excel(writer,'BR')
    database.to_excel(writer,engine='xlsxwriter')
    writer.save()
    print 'Database saved in xlsx format with file name %s'%filename

def database2csv(database,filename=None):
    filename=filename or 'db_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    database[database.acc_type=='RR'].to_csv('rr_%s'%filename)
    database[database.acc_type=='BR'].to_csv('br_%s'%filename)
    database[database.acc_type=='GO'].to_csv('go_%s'%filename)

    print 'Database saved in csv format with file name %s'%filename

def get_areacode(province): #needs to be based on both the province (and the city sometimes)
    return province.apply(lambda x : codes.get(x.lower(),None))

def province2abr(arg):
    return prov_abbr.get(arg.lower(),arg)

def city2abr(arg):
    return city_abbr.get(arg.lower(),arg)
#note: pdf if not clear on the placement of the or call in the crm for now it will be put in the city

def or_call(crm):
    crm.reset_index(drop=True,inplace=True)
    index=crm.duplicated(['name1','name2','City','Province','SAM_BLDNAME','SAM_STNAME'])
    skip=False
    for i in xrange(len(index)):
        if index.iloc[i] == True:
            line=crm.ix[i]
            prev_line=crm.ix[i-1]
            try:
                next_line=crm.ix[i+1]
            except:
                pass
            line['SAM_BLDNAME']=''
            line['SAM_STNMFR']=''
            line['SAM_STNAME']=''
            line['SAM_STSUBT']=''
            line['sam_estate']=''
            line['Province']=''
            line['class_code']=''
            line['class_desc']=''

            #line['name1']=''
            #line['name2']=''
            if skip == False:
                prev_line['Phone']=str(prev_line.Phone)+'/'+str(line.Phone)
                line['Phone']=None

                if index.ix[i-1] == False and index.ix[i+1]==True:
                    next_line['City']='Or Call:'
                else:
                    next_line['City']=''

                try:
                    if index.ix[i+1]    == True:
                        skip =True
                except:
                    pass
            else:
                skip = False
    #crm['Phone'].replace('', np.nan, inplace=True)
    return  crm[crm.Phone.notnull()]

def get_newest_db():
    try:
        newest=max(glob.iglob('database_backups/*.hdf5'), key=os.path.getctime)
    except:
        newest=None
    return newest

def apply_abbr(crm):
    crm.Province=crm.Province.apply(lambda x : province2abr(x))
    crm.City=crm.City.apply(lambda x : city2abr(x))
    return crm

def fix_duplicate(crm): #only does repeated numbers. Next needs to look for repeated adresses but not exact.
    crm.drop_duplicates('Phone',keep='last',inplace=True)

def add_product(crm):
    product=pd.read_excel('Product_Lookup.xlsx')
    product.rename(columns={'PROVINCE': 'Province', 'AREACODE': 'Areacode'}, inplace=True)
    pd.merge(crm,product, on=['Province','acc_type','Areacode'], how='left')

def add_class_code(database):
    classes=pd.read_excel('Company_Class.xlsx')
    pd.merge(database,classes, on=['Areacode','Phone','name1'], how='left')
