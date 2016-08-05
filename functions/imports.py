import pandas as pd
import time
import numpy as np
from functions.capitalization import titlecase
import csv
import glob
import os
import ConfigParser
import logging
pd.set_option('display.max_rows', None)
import warnings
warnings.filterwarnings('ignore',category=pd.io.pytables.PerformanceWarning)
from difflib import SequenceMatcher

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


Config = ConfigParser.ConfigParser()
Config.read("config/input_format")
#Config.read("config/areacodes") # NOT NEEDED
Config.read("config/prov_abbreviations")
Config.read("config/city_abbreviations")

prov_abbr=ConfigSectionMap('prov_abbreviations')
city_abbr=ConfigSectionMap('city_abbreviations')
#codes=ConfigSectionMap('areacodes') #NOT NEEEDED
crmout=open('config/crm_format', 'r')



def get_database(file):
    database=pd.read_hdf(file,'database')
    return  database

def update_database(update_file,database,bigbang=False):
    so_file=update_file
    logging.info("updating from; %s", so_file)

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
    update.fillna('', inplace=True)


    update['acc_type']=update['acc_type'].astype('category')
    #update['class_code']=''
    update=add_class_code(update)
    update['class_code']=update['class_code'].astype('str')

    update['src']=so_file.split('/')[-1]
    update['so_rangedate']=time.strftime("%Y-%m-%d")
    update['user']= os.getlogin()

    if bigbang == True:
        update.sort_values(by='last_name',inplace=True)
        update.drop_duplicates(names,inplace=True)
        update.reset_index(drop=True,inplace=True)
        return update
    OP=update[update.so_type == 'OP']
    IN=update[update.so_type == 'IN']
    IR=update[update.so_type == 'IR']
    CL=update[update.so_type == 'CL']
    NONE=update[update.so_type == '']
    database.loc[database.account_no.isin(OP.account_no), 'list_code']='CO'
    database.loc[database.account_no.isin(OP.account_no), 'src']=so_file.split('/')[-1]
    database.loc[database.account_no.isin(OP.account_no), 'so_rangedate']=time.strftime("%Y-%m-%d")
    database.loc[database.account_no.isin(OP.account_no), 'user']=os.getlogin()

    database.loc[database.account_no.isin(IR.account_no), 'list_code']='PB'
    database.loc[database.account_no.isin(IR.account_no), 'src']=so_file.split('/')[-1]
    database.loc[database.account_no.isin(IR.account_no), 'so_rangedate']=time.strftime("%Y-%m-%d")
    database.loc[database.account_no.isin(IR.account_no), 'user']=os.getlogin()

    database[database.account_no.isin(CL.account_no)].update(update)
    database=database.append(IN)
    if len(NONE) > 0:
        to_fwf(NONE.drop(['class_code','src','so_rangedate','user']),'no_sotype_%s.txt' % time.strftime("%Y-%m-%d"))
    logging.info("%i entries have been added to the database", len(IN.index))
    logging.info("%i entries have been updated in the database", sum(database.account_no.isin(CL.account_no)))
    logging.info("%i entries have been made PB", sum(database.account_no.isin(IR.account_no)))
    logging.info("%i entries have been made CO",sum(database.account_no.isin(OP.account_no)))
    logging.info('%i lines had no so_type and have been written for updating',len(NONE.index) )
    logging.debug('the following entries have been added to the database')
    logging.debug(IN.last_name)
    logging.debug('The following have been updated')
    logging.debug(database[database.account_no.isin(CL.account_no)].last_name)
    logging.debug('The following have been made PB')
    logging.debug(database[database.account_no.isin(IR.account_no)].last_name)
    logging.debug('The following have been made CO')
    logging.debug(database[database.account_no.isin(OP.account_no)].last_name)
    logging.debug('The following lines have no so_type and have been written for updating')
    logging.debug(NONE.last_name)

    database.sort_values(by='last_name',inplace=True)
    database.drop_duplicates(names,inplace=True)
    database.reset_index(drop=True,inplace=True)
    return database

def save_database(database,filename=None):
    filename=filename or 'database_backups/database-%s.hdf5'% time.strftime('%Y-%m-%d-%H-%M-%S')
    database.to_hdf(filename, 'database', mode='w', format='table')
    logging.info("Database has been saved as %s in the database_backups folder", filename)

#Input database as a  pandas dataframe and returns a pandas dataframe with data in CRM format
#Specify export = True if an excel export is required, in this case the function returns nothing
def create_residential_crm(database,export=False,filename=None,abbr=True):
    rr=database[(database.acc_type=='RR') & (database.list_code=='PB')]
    #rr=rr[rr.list_code=='PB']
    rr_crm=pd.DataFrame()
    rr_crm['Areacode']=rr.mem_wstd.str.slice(0,-7)#.astype('int64')#get_areacode(rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    rr_crm['Phone']=rr.mem_wstd.str.slice(-7)
    rr_crm['name1']=rr.last_name
    rr_crm['name2']=rr.first_name
    rr_crm['SAM_BLDNAME']=rr.sam_bldname
    rr_crm['SAM_STNMFR']=rr.sam_stnmfr
    rr_crm['SAM_STNAME']=rr.sam_stname
    rr_crm['SAM_STSUBT']=rr.sam_stsubt
    rr_crm['sam_estate']=rr.sam_estate
    rr_crm['City']=rr.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    rr_crm['Province']=rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    rr_crm['class_code']=rr.class_code
    rr_crm=add_product(rr_crm,'RR')
    #rr_crm.loc(rr_crm.SAM_STNAME=='' and ,)

    rr_crm.name1 = rr_crm.name1.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.name2 = rr_crm.name2.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.SAM_BLDNAME = rr_crm.SAM_BLDNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.SAM_STNAME = rr_crm.SAM_STNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.SAM_STSUBT = rr_crm.SAM_STSUBT.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.sam_estate = rr_crm.sam_estate.apply(lambda x: titlecase(x) if x.isupper() else x)
    rr_crm.City = rr_crm.City.apply(lambda x: titlecase(x))
    rr_crm.Province = rr_crm.Province.apply(lambda x: titlecase(x))
    #or call may not be necessary here
    fix_duplicate(rr_crm)
    rr_crm= or_call(rr_crm)
    if abbr == True:
        apply_abbr(rr_crm)
    if export == True:
        filename=filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        rr_crm.to_csv('rr_%s'%filename)
        #writer.save()
        logging.info('RR CRM saved in csv format with file name rr_%s',filename)
    logging.info('RR crm DONE')
    return rr_crm

def create_government_crm(database,export=False,filename=None,abbr=True):
    go=database[(database.acc_type=='GO') & (database.list_code=='PB')]
    #go=go[go.list_code=='PB']
    go_crm=pd.DataFrame()
    go_crm['Areacode']=go.mem_wstd.str.slice(0,-7)#.astype('int64')##get_areacode(go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    go_crm['Phone']=go.mem_wstd.str.slice(-7)
    go_crm['name1']=go.last_name
    go_crm['name2']=go.first_name
    go_crm['SAM_BLDNAME']=go.sam_bldname
    go_crm['SAM_STNMFR']=go.sam_stnmfr
    go_crm['SAM_STNAME']=go.sam_stname
    go_crm['SAM_STSUBT']=go.sam_stsubt
    go_crm['sam_estate']=go.sam_estate
    go_crm['City']=go.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    go_crm['Province']=go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    go_crm['class_code']=go.class_code
#    go_crm['acc_type']='GO'
    go_crm=add_product(go_crm,'GO')
    go_crm.name1 = go_crm.name1.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.name2 = go_crm.name2.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.SAM_BLDNAME = go_crm.SAM_BLDNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.SAM_STNAME = go_crm.SAM_STNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.SAM_STSUBT = go_crm.SAM_STSUBT.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.sam_estate = go_crm.sam_estate.apply(lambda x: titlecase(x) if x.isupper() else x)
    go_crm.City = go_crm.City.apply(lambda x: titlecase(x))
    go_crm.Province = go_crm.Province.apply(lambda x: titlecase(x))
    fix_duplicate(go_crm)
    go_crm= or_call(go_crm)
    if abbr == True:
        apply_abbr(go_crm)
    if export == True:
        filename = filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        go_crm.to_csv('go_%s'%filename)
        #writer.save()
        logging.info('GO CRM saved in csv format with file name go_%s', filename)
    logging.info('GO crm DONE')

    return go_crm

def create_buisness_crm(database,export=False,filename=None,abbr=True):
    br=database[(database.acc_type=='BR') & (database.list_code=='PB')]
    #br=br[br.list_code=='PB']
    br_crm=pd.DataFrame()
    br_crm['Areacode']=br.mem_wstd.str.slice(0,-7)#.astype('int64')##get_areacode(br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    br_crm['Phone']=br.mem_wstd.str.slice(-7)
    br_crm['name1']=br.last_name
    br_crm['name2']=br.first_name
    br_crm['SAM_BLDNAME']=br.sam_bldname
    br_crm['SAM_STNMFR']=br.sam_stnmfr
    br_crm['SAM_STNAME']=br.sam_stname
    br_crm['SAM_STSUBT']=br.sam_stsubt
    br_crm['sam_estate']=br.sam_estate
    br_crm['City']=br.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    br_crm['Province']=br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    br_crm['class_code']=br.class_code
#    br_crm['acc_type']='BR'
    br_crm=add_product(br_crm,'BR')
    br_crm.name1 = br_crm.name1.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.name2 = br_crm.name2.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.SAM_BLDNAME = br_crm.SAM_BLDNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.SAM_STNAME = br_crm.SAM_STNAME.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.SAM_STSUBT = br_crm.SAM_STSUBT.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.sam_estate = br_crm.sam_estate.apply(lambda x: titlecase(x) if x.isupper() else x)
    br_crm.City = br_crm.City.apply(lambda x: titlecase(x))
    br_crm.Province = br_crm.Province.apply(lambda x: titlecase(x))
    fix_duplicate(br_crm)
    #place orcall function here since we want LR and Lr to be duplicates
    br_crm= or_call(br_crm)
    if abbr == True:
        br_crm=apply_abbr(br_crm)
    if export == True:
        filename= filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        br_crm.to_csv('br_%s'%filename)
        #writer.save()
        logging.info( 'BR CRM saved in csv format with file name br_%s',filename)
    logging.info('BR crm DONE')

    return br_crm

def create_crm(database,filename=None):
    filename=filename or'crm_%s.xlsx' % time.strftime('%Y-%m-%d-%H-%M-%S')
    writer = pd.ExcelWriter(filename)
    create_residential_crm(database).to_excel(writer,'RR')
    create_government_crm(database).to_excel(writer,'GO')
    create_buisness_crm(database).to_excel(writer,'BR')
    writer.save()
    logging.info('CRM saved in xlsx format with file name %s',filename)

def create_crm_csv(database,filename=None,abbr=True):
    filename=filename or'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    create_residential_crm(database,export=True,filename=filename,abbr=abbr)
    create_government_crm(database,export=True,filename=filename,abbr=abbr)
    create_buisness_crm(database,export=True,filename=filename,abbr=abbr)
    logging.info('CRM saved in csv format with file names %s',filename)

def create_yellowpages_crm(database, filename=None):
    filename=filename or'yp_crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    database.class_code=database.class_code.replace('', np.nan)
    database=database[database.class_code.notnull()]
    br_crm=create_buisness_crm(database)
    br_crm.to_csv('br_%s'%filename)
    logging.info('Yellow Pages CRM saved in csv format with file names %s',filename)

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

def get_areacode(province): ###NOT NEEDED
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
                next_line=crm.iloc[i+1]
            except:
                pass
            crm.SAM_BLDNAME.iloc[i]=''
            crm.SAM_STNMFR.iloc[i]=''
            crm.SAM_STNAME.iloc[i]=''
            crm.SAM_STSUBT.iloc[i]=''
            crm.sam_estate.iloc[i]=''
            crm.Province.iloc[i]=''
            crm.class_code.iloc[i]=''
            crm.Product.iloc[i]=''

            if skip == False:
                crm.Phone.iloc[i-1]=str(prev_line.Phone)+'/'+str(line.Phone)
                crm.Phone.iloc[i]=np.nan
                if index.iloc[i-1] == False and index.iloc[i+1]==True:
                   crm.City.iloc[i+1]='Or Call:'
                else:
                   crm.City.iloc[i+1]=''

                try:
                    if index.iloc[i+1] == True:
                        skip =True
                except:
                    pass
            else:
                skip = False
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
    #make copy to change case and strip punctuation then check for duplicates. Use this as an index to remove from final crm output

def add_product(crm,acc_type):
    product=pd.read_excel('Product_Lookup.xlsx')
    product=product[product.acc_type == acc_type]
    product=product.drop('acc_type',1)
    product.rename(columns={'PROVINCE': 'Province', 'AREACODE': 'Areacode','PRODUCT SECTION':'Product'}, inplace=True)
    product.Province=product.Province.str.upper()
    crm.Areacode=crm.Areacode.astype('int64')
    crm = pd.merge(crm,product, on=['Province','Areacode'], how='inner')
    crm.Areacode=crm.Areacode.astype('object')
    return crm

def add_class_code(database):
    classes=pd.read_excel('Company_Class.xlsx')
    classes.rename(columns={'name1': 'last_name', 'name2': 'first_name'}, inplace=True)
    classes_up=classes.copy()
    classes_up.last_name=classes_up.last_name.str.upper()
    classes_up.first_name=classes_up.first_name.str.upper()
    classes_up.fillna(value='', inplace=True)
    classes_up.Phone=classes_up.Phone.astype('int64')
    classes_up.Areacode=classes_up.Areacode.astype('int64')

#classes_up.class_code=classes_up.class_code.astype('int64')
    #classes_up = classes_up.drop(['Areacode','Product'], 1)
    database_up=database.copy()
    database_up.last_name=database_up.last_name.str.upper()
    database_up.first_name=database_up.first_name.str.upper()
#database_up.update(classes_up)
    database_up['Phone']=database_up.mem_wstd.str.slice(-7).astype('int64')
    database_up['Areacode']=database_up.mem_wstd.str.slice(0,-7).astype('int64')
    database_coded=pd.merge(database_up,classes_up, on=['Areacode','Phone','last_name','first_name'], how='left')
    database_coded.last_name=database.last_name
    database_coded.first_name=database.first_name
    database_coded = database_coded.drop(['Phone','Areacode'], 1)
    database_coded.fillna(value='', inplace=True)

    return database_coded

def format_output(crm): #NOT DONE
    out=pd.DataFrame
    for line in crmout:
        out['%s'%field]= crm['%s'%field]

def find_similar_names(db,probability=0.6):
    prev_last='sda sdfasdfas'
    prev_first='srydfas dfasdfs'
    similar=[]
    for index in xrange(len(db)):
        if (probability < SequenceMatcher(None,db.iloc[index].last_name.upper(),prev_last).ratio() < 1) and ( probability < SequenceMatcher(None,db.iloc[index].first_name.upper(),prev_first).ratio()):
            similar.append(db.iloc[index-1].values)
            similar.append(db.iloc[index].values)
        prev_first=db.iloc[index].first_name.upper()
        prev_last=db.iloc[index].last_name.upper()
    similardb=pd.DataFrame(similar)
    return similardb.drop_duplicates()
#def find_exceptions():


def to_fwf(db,filename):
    formats=[]
    for field in Config.options('input_format'):
        formats.append('%-'+str(Config.get('input_format',field)) +'s')
    np.savetxt(filename, db.values, fmt=formats, delimiter='')
