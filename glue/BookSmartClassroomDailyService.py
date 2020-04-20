import binascii
import bisect
from datetime import datetime, timedelta,date

import re

import boto3
import gzip
import json
import sys
import uuid


from botocore.errorfactory import ClientError
from multiprocessing.dummy import Pool
from multiprocessing.pool import ThreadPool

#args = getResolvedOptions(sys.argv, ['IN_BUCKET','IN_PREFIX','IN_SUFFIX',
#                                     'IN_S3_FOLDERS',
#                                     'OUT_BUCKET', 'OUT_PREFIX', 'OUT_SUFFIX',
#                                     'RANGE_FROM','RANGE_TO',
#                                     'MAPPING_DIRECTORY',
#                                    'JORDAN_PROJECT_ID'])

IN_BUCKET = 'wrstudent'  # realtime-analytics-qa1
IN_PREFIX = ''  # awsma/events
IN_SUFFIX = '.gz'  # .gz
IN_S3_FOLDERS = 'incoming_data'.split(',')   #[6ca121399ba949809a4f8ac7b9f5b264]

OUT_BUCKET = 'wr-apps-data'  # wr-apps-data-dev
OUT_PREFIX = 'incoming_data/BookSmart'  # incoming_data/BookSmartPlus
OUT_SUFFIX = '.log'  # .log


MAPPING_DIRECTORY = 'dev/mappings/BookSmart'  # dev/mappings/BookSmartPlus




app = ''

s3_ = boto3.resource('s3')





def is_gzip(events):
    return binascii.hexlify(events[:2]) == b'1f8b'

def get_mappings():
    s3 = boto3.client('s3')
    print('Getting column mappings from S3')
    obj = s3.get_object(Bucket='student-data-pipeline',
                        Key= MAPPING_DIRECTORY + '/master_preprocessor_column_mappings.json')
    return json.load(obj['Body'])


mappings = get_mappings()

def get_target(event, key):
    path = key.split('.')
    while len(path) > 1 and path[0] in event != None:
        event = event[path[0]]
        path = path[1:]
    return event, '.'.join(path)

def get_value(event, key):
    target, target_key = get_target(event, key)
    return target.get(target_key)

def set_value(event, key, value):
    if value != None:
        target, target_key = get_target(event, key)
        target[target_key] = value

def del_key(event,key):
    target,target_key = get_target(event,key)
    target.pop(target_key,None)

def custom_app_mapping(event):

    country_list = [
        get_value(event, 'attributes.country_code'),
        get_value(event, 'attributes.country'),
        get_value(event, 'attributes.sim_country_code'),
        get_value(event, 'attributes.network_country_code'),
        get_value(event, 'attributes.geolocation_country_code')
    ]
    # get the first non null country code from the country list
    set_value(event, 'attributes.country_code', next((x for x in country_list if x), ''))


def map_event(event):
    #event_version = get_value(event, 'application.version_name')
    #mappings = get_mappings(event_version)
    for mapping in mappings:
        input = mapping['input']
        output = mapping['output']
        if input.get('columnName'):
            current_value = get_value(event, input.get('columnName'))
            if input.get('value') and output.get('value'):
                if current_value == input.get('value'):
                    set_value(event,output.get('columnName'),output.get('value'))
            else:
                # column renaming whiles retaining old column value
                # essential we save the old column value, remove the column name from dict
                # and create again with new column name and retained value
                value = input.get('value', current_value)
                del_key(event,input.get('columnName'))
                set_value(event, output.get('columnName'), value)
        else:
            set_value(event, output.get('columnName'), input.get('value'))

    custom_app_mapping(event)
    country = get_value(event,'attributes.country_code')
    if country not in ['', None]:
        set_value(event, 'attributes.country_code', country.upper())

    # in some of the versions, the values of application.title and application.version have been swapped  eg. v_1.3.1
    app_version = get_value(event,'application.title')
    if app_version[0].isdigit():
        set_value(event, 'application.version_name',app_version)

    return event

def get_out_filename(input_s3_key):
    """
    Generates output filename given the input key
    :param input_s3_key: str
        eg. awsma/events/ba8855fbe6184252a44b942e3d247862/2020/03/09/19/MainLibraryAndroid-PROD-Delivery-Stream-1-2020-03-09-19-55-29-cbed37a2-3d2e-4fa2-91c2-28585a572906.gz
    :return: str
        eg.  MainLibraryAndroid-PROD-Delivery-Stream-1-2020-03-09-19-55-29-cbed37a2-3d2e-4fa2-91c2-28585a572906.log
    """
    return input_s3_key.split('/')[-1].replace(IN_SUFFIX,OUT_SUFFIX)


def backup(obj):

    copy_source = {'Bucket':IN_BUCKET, 'Key': obj.key}
    x = obj.key.split('/')
    dest = obj.key.replace('incoming_data','backup_data/BookSmart')

    s3_.meta.client.copy(copy_source, IN_BUCKET, dest)
    print(dest)



def gen_dest_key(obj):
    x = obj.key.split('/')
    parent_dir,sub_dir, log_file_name = x[2], x[-2], x[-1]
    out_key =  f"{OUT_PREFIX}/EVENT_LOG_LIST/{parent_dir} | {sub_dir} | {log_file_name}"
    out_key = out_key.replace('.log.gz','.log').replace('.gz','.log')
    return out_key


def get_event_arrival_time(file_path):

    try:

        filename = file_path
        match = re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})', filename)
        date = int(datetime.strptime(match.group(), '%Y-%m-%d_%H-%M-%S').timestamp() * 1000)
        return date
    except:
        return None




def map_and_upload(obj):

    events = []
    file = s3_.Object(IN_BUCKET, obj.key).get()['Body'].read()
    dest_key = gen_dest_key(obj)
    if is_gzip(file):
        file = gzip.decompress(file)
    for line in file.decode().split('\n'):
        if line:
            event = json.loads(line)

            set_value(event, 'attributes.obj_key', dest_key)
            arrival_timestamp = get_event_arrival_time(obj.key)
            if arrival_timestamp is None:
                arrival_timestamp = get_value(event,'event_timestamp')
            set_value(event,'arrival_timestamp',arrival_timestamp)
            # take care of corrupted events in log file
            if get_value(event,'attributes.organization_code') not in ['',None]:
                events.append(json.dumps(map_event(event)))


    # do not process log files without valid event entries
    if len(events) > 0:
        backup(obj)
        s3_.Object(OUT_BUCKET,dest_key).put(Body='\n'.join(events))


def get_already_processed_logs():
    input_bucket = s3_.Bucket('wr-apps-data')
    processed_logs = set()
    prefix = 'archived_data/BookSmart/'
    for obj in input_bucket.objects.filter(Prefix =prefix):
        if 'EVENT_LOG_LIST' in obj.key:
            file_name = obj.key.split("/")[-1]
            x = file_name.split("|")[0].strip()
            processed_logs.add(x)
    return processed_logs


def delete_root_folder(all_keys):
    """
    Deletes root folder and all files given key
    :param all_keys:
    :return:
    """
    my_set = set()
    for item in all_keys:
        x = item.split('/')
        my_set.add(f'{x[0]}/{x[1]}/{x[2]}') #eg incoming/v_1.1/BSD_2020_03_02

    input_bucket = s3_.Bucket(IN_BUCKET)

    for item in my_set:
        for obj in input_bucket.objects.filter(Prefix = item):
            s3_.Object(input_bucket.name,obj.key).delete()



def preprocess_and_move_logs():
    all_keys = []
    input_bucket = s3_.Bucket(IN_BUCKET)
    processed_logs_list = get_already_processed_logs()
    for folder in IN_S3_FOLDERS:
        _prefix ='incoming_data/'
        for obj in input_bucket.objects.filter(Prefix=_prefix):


            if obj.key.endswith(IN_SUFFIX):
                x = obj.key.split("/")[2]
                if x not in processed_logs_list:
                    print(obj.key)
                    all_keys.append(obj)
    pool = Pool(4)
    pool.map(map_and_upload, all_keys)

    #delete_root_folder(all_keys)


def preprocess_and_move_logs_old():
    all_keys = []
    input_bucket = s3_.Bucket(IN_BUCKET)
    exclusion_list = ['v_1.1/','v_1.1.1/','v_1.1.2/','v_1.1.3/','v_1.1.4/','v_1.1.5/']

    for folder in IN_S3_FOLDERS:
        _prefix ='/'.join([IN_PREFIX,folder])
        for obj in input_bucket.objects.filter(Prefix=_prefix):

            if obj.key.endswith(IN_SUFFIX) and not any(s in obj.key for s in exclusion_list):
                print(obj.key)
                all_keys.append(obj)

    pool = Pool(4)
    pool.map(map_and_upload, all_keys)

def logs_exist(bucket, prefix='', suffix=''):

    my_bucket = s3_.Bucket(IN_BUCKET)
    inclusion_list = ['']

    found = False
    for folder in IN_S3_FOLDERS:


        _prefix = folder+'/'
        print('Searching in '+_prefix +'\n')
        for obj in my_bucket.objects.filter(Prefix = _prefix):

            if obj.key.endswith(suffix) :
                print('Found ' +obj.key +'\n')
                found = True
                break
        if found == True:
            break  #stop searching

    return found

if logs_exist(bucket=IN_BUCKET, prefix=IN_PREFIX, suffix=IN_SUFFIX):
    print ('Bucket {:s} {:s} prefix contains new {:s} files'.format(
        IN_BUCKET, IN_PREFIX, IN_SUFFIX))

    preprocess_and_move_logs()
else:
    sys.exit(1)