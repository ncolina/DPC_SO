import pandas as pd
import string
def get_database(file):
    database=pd.read_hdf(file)
    return  database

def update_database(update_file,database):
    so_file='fwso/sample txtfile.txt'
    widths = [4,2,15,10,30,100,4,30,3,7,1,30,30,2,20,2,2,10,9,50,3,30,50]
    update=pd.read_fwf(so_file,
                   header=None,
                   widths=widths,
                   names=['dir_code','so_type','sonumber','mem_wstd','first_name','last_name','sam_city'
                          ,'sam_stname','sam_stsubt','sam_stnmfr','sam_subd','sam_estate','sam_bldname','sam_aptcod','sam_aprmnt'
                          ,'list_code','acc_type','old_wstd','so_date','joint_user','regulatory_id','account_no','disttribution_code'],
                   converters = {'mem_wstd': str, 'sam_stnmfr':str,'account_no':str},
                   index_col=None
                  )
                  update = update.fillna('')

    update['acc_type']=update['acc_type'].astype('category')
    update['src']=so_file
    update['so_rangedate']=time.strftime("%Y-%m-%d")
    database.append(update)
    database.sort_values(by='last_name',inplace=True)
    database.reset_index(drop=True,inplace=True)
    return database

def create_residential_crm(database):
    rr=database[database.acc_type=='RR']
    rr_crm=pd.DataFrame()
    rr_crm['Areacode']=rr.mem_wstd.str.slice(0,1)
    rr_crm['Phone']=rr.mem_wstd.str.slice(-7)
    rr_crm['name1']=rr.last_name
    rr_crm['name2']=rr.first_name
    rr_crm['SAM_BLDNAME']=rr.sam_bldname
    rr_crm['SAM_STNMFR']=rr.sam_stnmfr
    rr_crm['SAM_STNAME']=rr.sam_stname
    rr_crm['SAM_STSUBT']=rr.sam_stsubt
    rr_crm['sam_estate']=rr.sam_estate
    rr_crm['City']=rr.distribution_code.str.split('    ',1).str.get(0)
    rr_crm['Province']=rr.distribution_code.str.split('    ',1).str.get(1)
    rr_crm['class_code']=""
    rr_crm['class_desc']=""
    rr_crm.name1 = rr_crm.name1.apply(lambda x: string.capwords(x))
    rr_crm.name2 = rr_crm.name2.apply(lambda x: string.capwords(x))
    rr_crm.SAM_BLDNAME = rr_crm.SAM_BLDNAME.apply(lambda x: string.capwords(x))
    rr_crm.SAM_STNAME = rr_crm.SAM_STNAME.apply(lambda x: string.capwords(x))
    rr_crm.SAM_STSUBT = rr_crm.SAM_STSUBT.apply(lambda x: string.capwords(x))
    rr_crm.sam_estate = rr_crm.sam_estate.apply(lambda x: string.capwords(x))
    rr_crm.City = rr_crm.City.apply(lambda x: string.capwords(x))
    rr_crm.Province = rr_crm.Province.apply(lambda x: string.capwords(x))
    return rr_crm

def create_government_crm(database):
    go=database[database.acc_type=='GO']
    go_crm=pd.DataFrame()
    go_crm['Areacode']=go.mem_wstd.str.slice(0,1)
    go_crm['Phone']=go.mem_wstd.str.slice(-7)
    go_crm['name1']=go.last_name
    go_crm['name2']=go.first_name
    go_crm['SAM_BLDNAME']=go.sam_bldname
    go_crm['SAM_STNMFR']=go.sam_stnmfr
    go_crm['SAM_STNAME']=go.sam_stname
    go_crm['SAM_STSUBT']=go.sam_stsubt
    go_crm['sam_estate']=go.sam_estate
    go_crm['City']=go.distribution_code.str.split('    ',1).str.get(0)
    go_crm['Province']=go.distribution_code.str.split('    ',1).str.get(1)
    go_crm['class_code']=""
    go_crm['class_desc']=""
    go_crm.name1 = go_crm.name1.apply(lambda x: string.capwords(x))
    go_crm.name2 = go_crm.name2.apply(lambda x: string.capwords(x))
    go_crm.SAM_BLDNAME = go_crm.SAM_BLDNAME.apply(lambda x: string.capwords(x))
    go_crm.SAM_STNAME = go_crm.SAM_STNAME.apply(lambda x: string.capwords(x))
    go_crm.SAM_STSUBT = go_crm.SAM_STSUBT.apply(lambda x: string.capwords(x))
    go_crm.sam_estate = go_crm.sam_estate.apply(lambda x: string.capwords(x))
    go_crm.City = go_crm.City.apply(lambda x: string.capwords(x))
    go_crm.Province = go_crm.Province.apply(lambda x: string.capwords(x))
    return go_crm

def create_buisness_crm(database):
    br=database[database.acc_type=='BR']
    br_crm=pd.DataFrame()
    br_crm['Areacode']=br.mem_wstd.str.slice(0,1)
    br_crm['Phone']=br.mem_wstd.str.slice(-7)
    br_crm['name1']=br.last_name
    br_crm['name2']=br.first_name
    br_crm['SAM_BLDNAME']=br.sam_bldname
    br_crm['SAM_STNMFR']=br.sam_stnmfr
    br_crm['SAM_STNAME']=br.sam_stname
    br_crm['SAM_STSUBT']=br.sam_stsubt
    br_crm['sam_estate']=br.sam_estate
    br_crm['City']=br.distribution_code.str.split('    ',1).str.get(0)
    br_crm['Province']=br.distribution_code.str.split('    ',1).str.get(1)
    br_crm['class_code']=""
    br_crm['class_desc']=""
    br_crm.name1 = br_crm.name1.apply(lambda x: string.capwords(x))
    br_crm.name2 = br_crm.name2.apply(lambda x: string.capwords(x))
    br_crm.SAM_BLDNAME = br_crm.SAM_BLDNAME.apply(lambda x: string.capwords(x))
    br_crm.SAM_STNAME = br_crm.SAM_STNAME.apply(lambda x: string.capwords(x))
    br_crm.SAM_STSUBT = br_crm.SAM_STSUBT.apply(lambda x: string.capwords(x))
    br_crm.sam_estate = br_crm.sam_estate.apply(lambda x: string.capwords(x))
    br_crm.City = br_crm.City.apply(lambda x: string.capwords(x))
    br_crm.Province = br_crm.Province.apply(lambda x: string.capwords(x))
    return br_crm


def create_crm():


def database2xls(database)

def create_ouput_crm():

def place_to_code(arg):
    codes={
        'Kalookan City':'KC',
        'Las Pinas':'LP',
        'Makati':'Mkti',
        'Malabon':'Mal',
        'Mandaluyong':'Mand',
        'Manila':'Mla',
        'Marikina':'Mkna',
        'Muntinlupa':'Munt'
        'Marikina':'Mkna',
        
    }
    return codes.get(arg,arg)
