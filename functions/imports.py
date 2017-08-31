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
Config.read("config/sam_stsubt")
Config.read("config/sam_stname-abbr")
prov_abbr=ConfigSectionMap('prov_abbreviations')
city_abbr=ConfigSectionMap('city_abbreviations')
stsubt_abbr=ConfigSectionMap('sam_stsubt')
stname_abbr=ConfigSectionMap('sam_stname-abbr')
#codes=ConfigSectionMap('areacodes') #NOT NEEEDED
crmout=open('config/crm_format', 'r')



def get_database(file):
    database=pd.read_hdf(file,'database')
    return  database

def update_database(update_file,database,bigbang=False):
    so_file=update_file

    logging.info("Loading from; %s", so_file)

    names = Config.options('input_format')
    widths=[int(Config.get('input_format',name)) for name in names ]
    update=pd.read_fwf(so_file,
                   header=None,
                   widths=widths,
                   names=names,
                   converters = {'mem_wstd': str, 'sam_stnmfr':str,'account_no':str,'old_wstd':str,'so_date':str,'acc_type':str,'list_code':str},
                   #error_bad_lines=True,
                   #dtype='object',
                   index_col=None,
                   encoding='U8'
                  )
    update.fillna('', inplace=True)
    if (update['list_code'].iloc[0] != 'PB') and (update['list_code'].iloc[0] != 'CO'):
        print update['list_code'].iloc[0]
        raise "Input file spacing is off. Please check."
    update=find_exceptions(update)
    update['acc_type']=update['acc_type']#.astype('category')
    update['class_code']=''
    #update=add_class_code(update)
    update['class_code']=update['class_code'].astype('str')

    update['src']=so_file.split('/')[-1]
    update['so_rangedate']=time.strftime("%Y-%m-%d")
    update['user']= os.getlogin()

    joints=update[update['joint_user']!=''].copy()
    joints['last_name']=joints['joint_user']
    joints['joint_user']=''
    joints['first_name']=''
    joints['list_code']='PB'
    update=update.append(joints)
    if bigbang == True:
        update.sort_values(by=['last_name','first_name','sam_city','sam_stname','sam_bldname','sam_estate'],inplace=True)
        update.drop_duplicates(names,inplace=True)
        update.reset_index(drop=True,inplace=True)
        return update
    OP=update[update.so_type == 'OP']
    IN=update[update.so_type == 'IN']
    IR=update[update.so_type == 'IR']
    CL=update[update.so_type == 'CL']
    NONE=update[update.so_type == '']
    database.loc[database.mem_wstd.isin(OP.mem_wstd), 'list_code']='CO'
    database.loc[database.mem_wstd.isin(OP.mem_wstd), 'src']=so_file.split('/')[-1]
    database.loc[database.mem_wstd.isin(OP.mem_wstd), 'so_rangedate']=time.strftime("%Y-%m-%d")
    database.loc[database.mem_wstd.isin(OP.mem_wstd), 'user']=os.getlogin()

    database.loc[database.mem_wstd.isin(IR.mem_wstd), 'list_code']='PB'
    database.loc[database.mem_wstd.isin(IR.mem_wstd), 'src']=so_file.split('/')[-1]
    database.loc[database.mem_wstd.isin(IR.mem_wstd), 'so_rangedate']=time.strftime("%Y-%m-%d")
    database.loc[database.mem_wstd.isin(IR.mem_wstd), 'user']=os.getlogin()
    if len(database.account_no.isin(CL.account_no))>0:
        database = database[~database.mem_wstd.isin(CL.mem_wstd)]
        database = database.append(CL)
    database=database.append(IN)
    no_db_entry=pd.DataFrame()
    no_db_entry=no_db_entry.append(OP)
    no_db_entry=no_db_entry.append(IR)
    no_db_entry=no_db_entry.append(CL)
    no_db_entry=no_db_entry[(~no_db_entry.mem_wstd.isin(database.mem_wstd))]# &(~no_db_entry.account_no.isin(database.account_no))&(~no_db_entry.account_no.isin(database.account_no))]
    database=database.append(no_db_entry)

    if len(NONE) > 0:
        to_fwf(NONE.drop(['class_code','src','so_rangedate','user'],axis=1),'no_sotype_%s.txt' % time.strftime("%Y-%m-%d"))
    logging.info("%i entries have been added to the database", len(IN.index))
    logging.info("%i entries have been updated in the database", sum(database.mem_wstd.isin(CL.mem_wstd)))
    logging.info("%i entries have been made PB", sum(database.mem_wstd.isin(IR.mem_wstd)))
    logging.info("%i entries have been made CO",sum(database.mem_wstd.isin(OP.mem_wstd)))
    logging.info('%i lines were marked for update but were not in the database. They have been inserted in the database',len(no_db_entry.index) )

    logging.info('%i lines had no so_type and have been written for updating',len(NONE.index) )

    logging.debug('the following entries have been added to the database')
    logging.debug(IN.last_name)
    logging.debug('The following have been updated')
    logging.debug(database[database.mem_wstd.isin(CL.mem_wstd)].last_name)
    logging.debug('The following have been made PB')
    logging.debug(database[database.mem_wstd.isin(IR.mem_wstd)].last_name)
    logging.debug('The following have been made CO')
    logging.debug(database[database.mem_wstd.isin(OP.mem_wstd)].last_name)
    logging.debug('The following have been marked for update but are not in the db. They have been inserted into the db.')
    logging.debug(no_db_entry.last_name)
    logging.debug('The following lines have no so_type and have been written for updating')
    logging.debug(NONE.last_name)

    database.sort_values(by=['last_name','first_name','sam_stname'],inplace=True)
    database.drop_duplicates(names,inplace=True)
    database.reset_index(inplace=True,drop=True)
    return database

def save_database(database,filename=None):
    filename=filename or 'database_backups/database-%s.hdf5'% time.strftime('%Y-%m-%d-%H-%M-%S')
    database.to_hdf(filename, 'database', mode='w', format='fixed')
    logging.info("Database has been saved as %s in the database_backups folder", filename)

#Input database as a  pandas dataframe and returns a pandas dataframe with data in CRM format
#Specify export = True if an excel export is required, in this case the function returns nothing

def create_residential_crm(database,export=False,filename=None,abbr=True,multi_or=False,cap=True):
    rr=database[database.acc_type=='RR']
    rr=rr[rr.list_code=='PB']
    #rr=rr[rr.list_code=='PB']
    rr_crm=pd.DataFrame()
    rr_crm['Areacode']=rr.mem_wstd.str.slice(0,-7)#.astype('int64')#get_areacode(rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    rr_crm['Phone']=rr.mem_wstd.str.slice(-7)
    rr_crm['name1']=rr.last_name.astype('str')
    rr_crm['name2']=rr.first_name.astype('str')
    rr_crm['SAM_BLDNAME']=''#rr.sam_bldname
    rr_crm['SAM_STNMFR']=rr.sam_stnmfr.astype('str')
    rr_crm['SAM_STNAME']=rr.sam_stname.astype('str')
    rr_crm['SAM_STSUBT']=rr.sam_stsubt.astype('str')
    rr_crm['sam_estate']=rr.sam_estate.astype('str')
    rr_crm['City']=rr.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    rr_crm['Province']=rr.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    rr_crm=add_product(rr_crm,'RR')
    rr_crm['class_code']=rr.class_code
    rr_crm['class_desc']=''

    #rr_crm.loc(rr_crm.SAM_STNAME=='' and ,)
    if cap== True:
        rr_crm.name1 = rr_crm.name1.apply(lambda x: titlecase(x.lower())) # Only in the residential we don't look for stylized names
        rr_crm.name2 = rr_crm.name2.apply(lambda x: titlecase(x.lower()))
        #rr_crm.SAM_BLDNAME = rr_crm.SAM_BLDNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    rr_crm.SAM_STNAME = rr_crm.SAM_STNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    rr_crm.SAM_STSUBT = rr_crm.SAM_STSUBT.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    rr_crm.sam_estate = rr_crm.sam_estate.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    rr_crm.City = rr_crm.City.apply(lambda x: titlecase(x))
    rr_crm.Province = rr_crm.Province.apply(lambda x: titlecase(x))
    rr_crm.SAM_STSUBT=rr_crm.SAM_STSUBT.apply(lambda x : expand_stsubt(x))

    titles_file =open("config/titles",'r')
    TITLES = str(titles_file.read().rstrip('\n'))
    rr_crm.name1=rr_crm.name1.str.replace(r'(%s)\W'%TITLES,'')
    rr_crm.name2=rr_crm.name2.str.replace(r'(%s)\W'%TITLES,'')

    rr_crm.name1=rr_crm.name1.str.replace(r'[.,;:]',' ')
    rr_crm.name2=rr_crm.name2.str.replace(r'[.,;:]',' ')

    rr_crm=remove_st(rr_crm)

    fix_duplicate(rr_crm)
    rr_crm= or_call(rr_crm,multi_or=multi_or)
    if abbr == True:
        apply_abbr(rr_crm)
    if export == True:

        filename=filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        rr_crm.to_csv('rr_%s'%filename,index=False)
        #writer.save()
        logging.info('RR CRM saved in csv format with file name rr_%s',filename)
    logging.info('RR crm DONE')
    return rr_crm

def create_government_crm(database,export=False,filename=None,abbr=True,multi_or=False,name_sub=True, cap=True):
    go=database[database.acc_type=='GO']
    go=go[go.list_code=='PB']
    #go=go[go.list_code=='PB']
    go_crm=pd.DataFrame()
    go_crm['Areacode']=go.mem_wstd.str.slice(0,-7)#.astype('int64')##get_areacode(go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    go_crm['Phone']=go.mem_wstd.str.slice(-7)
    go_crm['name1']=go.last_name.astype('str')
    go_crm['name2']=go.first_name.astype('str')
    go_crm['SAM_BLDNAME']=''#go.sam_bldname
    go_crm['SAM_STNMFR']=go.sam_stnmfr.astype('str')
    go_crm['SAM_STNAME']=go.sam_stname.astype('str')
    go_crm['SAM_STSUBT']=go.sam_stsubt.astype('str')
    go_crm['sam_estate']=''#go.sam_estate.astype('str')
    go_crm['City']=go.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    go_crm['Province']=go.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    go_crm=add_product(go_crm,'GO')
    go_crm['class_code']=go.class_code
    go_crm['class_desc']=''



    #    go_crm['acc_type']='GO'
    if cap == True:
        go_crm.name1 = go_crm.name1.apply(lambda x: titlecase(x.lower())) # if x.isupper() else x)
        go_crm.name2 = go_crm.name2.apply(lambda x: titlecase(x.lower())) # if x.isupper() else x)
    go_crm.SAM_BLDNAME = ''#go_crm.SAM_BLDNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    go_crm.SAM_STNAME = go_crm.SAM_STNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    go_crm.SAM_STSUBT = go_crm.SAM_STSUBT.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    go_crm.sam_estate = go_crm.sam_estate.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    go_crm.City = go_crm.City.apply(lambda x: titlecase(x))
    go_crm.Province = go_crm.Province.apply(lambda x: titlecase(x))

# remove the punctuation
    go_crm.name1=go_crm.name1.str.replace(r'[.,;:]',' ')
    go_crm.name2=go_crm.name2.str.replace(r'[.,;:]',' ')
    if name_sub==True:
        go_crm=expand_abbr(go_crm)
    go_crm=remove_st(go_crm)

    fix_duplicate(go_crm)
    go_crm=remove_st(go_crm)

    go_crm= or_call(go_crm,multi_or=multi_or)
    if abbr == True:
        apply_abbr(go_crm)
    if export == True:
        filename = filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        go_crm.to_csv('go_%s'%filename,index=False)
        #writer.save()
        logging.info('GO CRM saved in csv format with file name go_%s', filename)
    logging.info('GO crm DONE')

    return go_crm

def create_buisness_crm(database,export=False,filename=None,abbr=True,multi_or=False,name_sub=True,cap=True):
    br=database[database.acc_type=='BR']
    br=br[br.list_code=='PB']
    #br=br[br.list_code=='PB']
    br_crm=pd.DataFrame()
    br_crm['Areacode']=br.mem_wstd.str.slice(0,-7)#.astype('int64')##get_areacode(br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    br_crm['Phone']=br.mem_wstd.str.slice(-7)
    br_crm['name1']=br.last_name.astype('str')
    br_crm['name2']=br.first_name.astype('str')
    br_crm['SAM_BLDNAME']=br.sam_bldname.astype('str')
    br_crm['SAM_STNMFR']=br.sam_stnmfr.astype('str')
    br_crm['SAM_STNAME']=br.sam_stname.astype('str')
    br_crm['SAM_STSUBT']=br.sam_stsubt.astype('str')
    br_crm['sam_estate']=br.sam_estate.astype('str')
    br_crm['City']=br.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    br_crm['Province']=br.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    #    br_crm['acc_type']='BR'
    br_crm=add_product(br_crm,'BR')
    br_crm['class_code']=br.class_code
    br_crm['class_desc']=""
    if cap==True:
        br_crm.name1 = br_crm.name1.apply(lambda x: titlecase(x.lower())) # if x.isupper() else x)
        br_crm.name2 = br_crm.name2.apply(lambda x: titlecase(x.lower())) # if x.isupper() else x)
    br_crm.SAM_BLDNAME = br_crm.SAM_BLDNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    br_crm.SAM_STNAME = br_crm.SAM_STNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    br_crm.SAM_STSUBT = br_crm.SAM_STSUBT.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    br_crm.sam_estate = br_crm.sam_estate.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    br_crm.City = br_crm.City.apply(lambda x: titlecase(x))
    br_crm.Province = br_crm.Province.apply(lambda x: titlecase(x))

#remove buliding name if the same as company name
    br_crm.loc[(br_crm.SAM_BLDNAME==br_crm.name1),'SAM_BLDNAME']=''
# remove st information if building name exists and is not the same as the company name
    br_crm.loc[(br_crm.SAM_BLDNAME!=''),'SAM_STNAME']=''
    br_crm.loc[(br_crm.SAM_BLDNAME!=''),'SAM_STSUBT']=''
    br_crm.loc[(br_crm.SAM_BLDNAME!=''),'sam_estate']=''
    br_crm.loc[(br_crm.SAM_BLDNAME!=''),'SAM_STNMFR']=''

# remove the punctuation
    br_crm.name1=br_crm.name1.str.replace(r'[.,;:]',' ')
    br_crm.name2=br_crm.name2.str.replace(r'[.,;:]',' ')
    if name_sub==True:
        br_crm=expand_abbr(br_crm)
    br_crm=remove_st(br_crm)

    fix_duplicate(br_crm)
    #place orcall function here since we want LR and Lr to be duplicates
    br_crm= or_call(br_crm,multi_or=multi_or)
    if abbr == True:
        br_crm=apply_abbr(br_crm)
    if export == True:

        filename= filename or 'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
        #writer = pd.ExcelWriter(filename)
        br_crm.to_csv('br_%s'%filename,index=False)
        #writer.save()

        logging.info( 'BR CRM   saved in csv format with file name br_%s',filename)
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

def create_crm_csv(database,filename=None,abbr=True,multi_or=False,name_sub=True,cap=True):
    filename=filename or'crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    create_residential_crm(database,export=True,filename=filename,abbr=abbr,multi_or=multi_or,cap=cap)
    create_government_crm(database,export=True,filename=filename,abbr=abbr,multi_or=multi_or,name_sub=name_sub,cap=cap)
    create_buisness_crm(database,export=True,filename=filename,abbr=abbr, multi_or=multi_or,name_sub=name_sub,cap=cap)
    logging.info('CRM saved in csv format with file names %s',filename)

def create_yellowpages_crm(database,filename=None,abbr=True,multi_or=False,name_sub=True,cap=True):
    yp=yp_crm_code(database)
    #br=br[br.list_code=='PB']
    yp_crm=pd.DataFrame()
    yp_crm['Areacode']=yp.mem_wstd.str.slice(0,-7)#.astype('int64')##get_areacode(yp.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()))
    yp_crm['Phone']=yp.mem_wstd.str.slice(-7)
    yp_crm['name1']=yp.last_name.astype('str')
    yp_crm['name2']=yp.first_name.astype('str')
    yp_crm['SAM_BLDNAME']=yp.sam_bldname.astype('str')
    yp_crm['SAM_STNMFR']=yp.sam_stnmfr.astype('str')
    yp_crm['SAM_STNAME']=yp.sam_stname.astype('str')
    yp_crm['SAM_STSUBT']=yp.sam_stsubt.astype('str')
    yp_crm['sam_estate']=yp.sam_estate.astype('str')
    yp_crm['City']=yp.distribution_code.str.split('    ',1).str.get(1).apply(lambda x : x.strip()).str.upper()
    yp_crm['Province']=yp.distribution_code.str.split('    ',1).str.get(0).apply(lambda x : x.strip()).str.upper()
    #    yp_crm['acc_type']='yp'
    yp_crm=add_product(yp_crm,'YP')
    #yp_crm['Product']=yp_crm['Product'].astype('str').str[:-2] + 'YP'
    yp_crm['class_code']=yp.class_code.astype('str')
    yp_crm['class_desc']=''
    if cap==True:
        yp_crm.name1 = yp_crm.name1.apply(lambda x: titlecase(x.lower())) #if x.isupper() else x)
        yp_crm.name2 = yp_crm.name2.apply(lambda x: titlecase(x.lower())) #if x.isupper() else x)
    yp_crm.SAM_BLDNAME = yp_crm.SAM_BLDNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    yp_crm.SAM_STNAME = yp_crm.SAM_STNAME.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    yp_crm.SAM_STSUBT = yp_crm.SAM_STSUBT.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    yp_crm.sam_estate = yp_crm.sam_estate.apply(lambda x: titlecase(x.lower()) if x.isupper() else x)
    yp_crm.City = yp_crm.City.apply(lambda x: titlecase(x))
    yp_crm.Province = yp_crm.Province.apply(lambda x: titlecase(x))

#remove buliding name if the same as company name
    yp_crm.loc[(yp_crm.SAM_BLDNAME==yp_crm.name1),'SAM_BLDNAME']=''
# remove st information if building name exists and is not the same as the company name
    yp_crm.loc[(yp_crm.SAM_BLDNAME!=''),'SAM_STNAME']=''
    yp_crm.loc[(yp_crm.SAM_BLDNAME!=''),'SAM_STSUBT']=''
    yp_crm.loc[(yp_crm.SAM_BLDNAME!=''),'sam_estate']=''
    yp_crm.loc[(yp_crm.SAM_BLDNAME!=''),'SAM_STNMFR']=''

# remove the punctuation
    yp_crm.name1=yp_crm.name1.str.replace(r'[.,;:]',' ')
    yp_crm.name2=yp_crm.name2.str.replace(r'[.,;:]',' ')
    if name_sub==True:
        yp_crm=expand_abbr(yp_crm)
    yp_crm=remove_st(yp_crm)
    fix_duplicate(yp_crm)
    yp_crm= or_call(yp_crm,multi_or=multi_or)
    if abbr == True:
        yp_crm=apply_abbr(yp_crm)
    filename=filename or'yp_crm_%s.csv' % time.strftime('%Y-%m-%d-%H-%M-%S')
    yp_crm.to_csv('yp_%s'%filename,index=False)
    logging.info('yp crm DONE')
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
def expand_stsubt(arg):
    return stsubt_abbr.get(arg.lower(),arg)

def remove_st(crm):
    crm.SAM_STSUBT[~(crm.SAM_STNAME.astype('str').str[0].astype('str').str.isnumeric())&(crm.SAM_STSUBT.str.contains('St',case=0,na=False))]=''
    crm.sam_estate[~(crm.SAM_STNAME=='')]=''
    return crm

def or_call(crm, multi_or=False):
    crm.sort_values(by=['name1','name2','Areacode','City','Province','SAM_STNAME','SAM_BLDNAME'],inplace=True)
    crm.reset_index(drop=True,inplace=True)
    index=crm.duplicated(['name1','name2','City','Province','SAM_STNAME','SAM_BLDNAME','Areacode'])
    skip=False
    for i in xrange(len(index)):
        if index.iloc[i] == True:
            line=crm.iloc[i]
            prev_line=crm.iloc[i-1]
            try:
                next_line=crm.iloc[i+1]
            except:
                skip=True

                if multi_or==False:
                    if index.iloc[i-1]==False:
                        crm.SAM_STNAME.iloc[i]='Or Call'
                    else:
                        crm.SAM_STNAME.iloc[i]=''
                else:
                        crm.SAM_STNAME.iloc[i]='Or Call'

            try:
                crm.SAM_BLDNAME.iloc[i]=''
            except:
                pass
            crm.SAM_STNMFR.iloc[i]=''
            #crm.SAM_STNAME.iloc[i]=''
            crm.SAM_STSUBT.iloc[i]=''
            crm.sam_estate.iloc[i]=''
            crm.Province.iloc[i]=''
            crm.City.iloc[i]=''

            crm.class_code.iloc[i]=''
            #crm.Product.iloc[i]=''

            if skip == False:
                if index.iloc[i-1]==False and index.iloc[i+1]==False:
                    crm.SAM_STNAME.iloc[i]='Or Call'
                else:
                    if index.iloc[i+1]==True:
                        crm.Phone.iloc[i]=str(line.Phone)+'/'+str(next_line.Phone)
                        crm.Phone.iloc[i+1]=np.nan
                    if multi_or==False:
                        try:
                            if index.iloc[i-1] == False: #and index.iloc[i+1]==True:
                                crm.SAM_STNAME.iloc[i]='Or Call'
                            else:#if index.iloc[i+1]==True:
                                crm.SAM_STNAME.iloc[i]=''
                        except:
                            pass
                    else:
                        #try:
                        #    if index.iloc[i+1]==True:
                        crm.SAM_STNAME.iloc[i]='Or Call'
                #        except:
                #            pass
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
    for key, value in stname_abbr.iteritems():
        crm.SAM_STNAME=crm.SAM_STNAME.str.replace(r'(?<!\w)({0})(?=\W|$)'.format(key), value, case=False)
        crm.SAM_BLDNAME=crm.SAM_BLDNAME.str.replace(r'(?<!\w)({0})(?=\W|$)'.format(key), value, case=False)

    return crm

def expand_abbr(crm):
    crm.name1=crm.name1.str.replace(r'(?<!\w)(corp)(?=\W|$)', 'Corporation', case=False)
    crm.name1=crm.name1.str.replace(r'(?<!\w)(inc)(?=\W|$)', 'Incorporated', case=False)
    crm.name1=crm.name1.str.replace(r'(?<!\w)(co)(?=\W|$)', 'Company', case=False)
    crm.name1=crm.name1.str.replace(r'(?<!\w)&(?!\w)', 'and', case=False)
    crm.name1=crm.name1.str.replace(r'(?<!\w)(phil)(?=\W|$)', 'Philippine', case=False)
    crm.name1=crm.name1.str.replace(r'(?<!\w)(phils)(?=\W|$)', 'Philippines', case=False)
    crm.name1=crm.name1.str.replace(r"(?<!\w)(int'l)(?=\W|$)", 'International', case=False)
    crm.SAM_STSUBT=crm.SAM_STSUBT.apply(lambda x : expand_stsubt(x))
    return crm

def fix_duplicate(crm): #only does repeated numbers. Next needs to look for repeated adresses but not exact.
    crm.drop_duplicates(['Phone','name1','name2'],keep='last',inplace=True)
    crm.name1=crm.name1.str.replace(r'\s{2,10}', ' ')
    crm.name2=crm.name2.str.replace(r'\s{2,10}', ' ')

    #make copy to change case and strip punctuation then check for duplicates. Use this as an index to remove from final crm output

def add_product(crm,acc_type):
    product=pd.read_excel('Product_Lookup.xlsx')
    product=product[product.acc_type == acc_type]
    product=product.drop('acc_type',1)
    product.rename(columns={'PROVINCE': 'Province', 'AREACODE': 'Areacode','PRODUCT SECTION':'Product'}, inplace=True)
    product.Province=product.Province.str.upper()
    crm.Areacode=crm.Areacode.astype('int64')
    crm = pd.merge(crm,product, on=['Province','Areacode'], how='left')
    crm.Areacode=crm.Areacode.astype('object')

    #check if the product is blank. if so, output line info
    no_product=crm[crm.Product=='']
    if len(no_product)>0:
        logging.info('There are %s entries without products', len(no_product))
        logging.debug('The following are the entries without products:')
        logging.debug(no_product)
    return crm

def yp_crm_code(database):
    classes=pd.read_csv('Company_Class.csv',converters={'name1':str,'name2':str,'class_code':str})
    classes.rename(columns={'name1': 'last_name', 'name2': 'first_name','SAM_STNAME':'sam_stname','SAM_BLDNAME':'sam_bldname','SAM_STNMFR':'sam_stnmfr','SAM_STSUBT':'sam_stsubt'}, inplace=True)
    classes.drop(['sam_stname','sam_bldname','sam_stsubt','sam_stnmfr'],axis=1, inplace=True)
    classes_up=classes#.copy()
    classes_up.last_name=classes_up.last_name.str.upper()
    classes_up.first_name=classes_up.first_name.str.upper()
    #classes_up.sam_stname=classes_up.sam_stname.str.upper()
    #classes_up.sam_bldname=classes_up.sam_bldname.str.upper()
    #classes_up.sam_stsubt=classes_up.sam_stsubt.str.upper()
    classes_up.Phone=classes_up.Phone.astype('float64')
    classes_up.Areacode=classes_up.Areacode.astype('float64')
    classes_up.class_code=classes_up.class_code.astype('str')
    classes_up.fillna(value='', inplace=True)
    #classes_up = classes_up.drop(['Areacode','Product'], 1)
    database_up=database#.copy()
    database_up.last_name=database_up.last_name.str.upper()
    database_up.first_name=database_up.first_name.str.upper()
    #database_up.sam_stname=database_up.sam_stname.str.upper()
    #database_up.sam_bldname=database_up.sam_bldname.str.upper()
    #database_up.sam_stsubt=database_up.sam_stsubt.str.upper()
    #database_up = database_up.drop('Product', 1)
    #database_up.update(classes_up)
    database_up['Phone']=database_up.mem_wstd.str.slice(-7).astype('float64')
    database_up['Areacode']=database_up.mem_wstd.str.slice(0,-7).astype('float64')
    database_coded=pd.merge(database_up,classes_up, on=['Areacode','Phone','last_name','first_name'], how='inner')
    #database_coded=pd.merge(database_coded,classes_up)
    #database_coded.last_name=database.last_name
    #database_coded.first_name=database.first_name
    #database_coded.sam_stname=database.sam_stname
    #database_coded.sam_bldname=database.sam_bldname
    #database_coded.sam_stnmfr=database.sam_stnmfr
    #database_coded.sam_stname=database.sam_stname
    database_coded = database_coded.drop(['Phone','Areacode','class_code_x'], 1)
    database_coded.rename(columns={'class_code_y':'class_code'}, inplace=True)

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

def find_exceptions(update):
    exception_list1=update.last_name.str.contains(r'^[-!$%^&*_+|~=`{}[\]:/;<>?,.@#]|^$|[\xA0-\xC8]|[\xCA-\xD0]|[\xD2-\xE8]|[\xEA-\xF0]|[\xF2-\xFF]', regex=True)
    exception_list2=update.mem_wstd.str.contains('.', regex=False)
    exceptions=update[exception_list1 | exception_list2 ]
    if len(exceptions)>0:
        filename='exceptions-%s.txt' % time.strftime('%Y-%m-%d-%H-%M-%S')
        to_fwf(exceptions,filename)
        logging.info('%i lines have been tagged as exceptions and have been written to %s'%(len(exceptions),filename))
        logging.debug('The following are marked as exceptions')
        logging.debug(exceptions.last_name)
    return update[~(exception_list1|exception_list2)]

def to_fwf(db,filename):
    formats=[]
    for field in Config.options('input_format'):
        formats.append('%-'+str(Config.get('input_format',field)) +'s')
    np.savetxt(filename, db.values, fmt=formats, delimiter='')
