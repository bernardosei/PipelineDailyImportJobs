import binascii
import bisect
from datetime import datetime, timedelta,date
from awsglue.utils import getResolvedOptions
import re

import boto3
import gzip
import json
import sys
import uuid


from botocore.errorfactory import ClientError
from multiprocessing.dummy import Pool
from multiprocessing.pool import ThreadPool

args = getResolvedOptions(sys.argv, ['IN_BUCKET','IN_PREFIX','IN_SUFFIX',
                                     'IN_S3_FOLDERS',
                                     'OUT_BUCKET', 'OUT_PREFIX', 'OUT_SUFFIX',
                                     'RANGE_FROM','RANGE_TO',
                                     'MAPPING_DIRECTORY',
                                     'JORDAN_PROJECT_ID'])

IN_BUCKET = args['IN_BUCKET']  # realtime-analytics-qa1
IN_PREFIX = args['IN_PREFIX']  # awsma/events
IN_SUFFIX = args['IN_SUFFIX']  # .gz
IN_S3_FOLDERS = args['IN_S3_FOLDERS'].split(',')   #[6ca121399ba949809a4f8ac7b9f5b264]

OUT_BUCKET = args['OUT_BUCKET']  # wr-apps-data-dev
OUT_PREFIX = args['OUT_PREFIX']  # incoming_data/BookSmartPlus
OUT_SUFFIX = args['OUT_SUFFIX']  # .log


MAPPING_DIRECTORY = args['MAPPING_DIRECTORY']  # dev/mappings/BookSmartPlus
JORDAN_PROJECT_ID = args['JORDAN_PROJECT_ID']
RANGE_FROM = args['RANGE_FROM']
RANGE_TO = args['RANGE_TO']

if 'PREVIOUS_DAY' in [RANGE_FROM,RANGE_TO]:

    date_previous_day = datetime.now() - timedelta(days =1)
    RANGE_FROM = date_previous_day.strftime("%Y-%m-%d")
    RANGE_TO = date_previous_day.strftime("%Y-%m-%d")

RANGE_FROM = datetime.strptime(RANGE_FROM, "%Y-%m-%d")
RANGE_TO = datetime.strptime(RANGE_TO, "%Y-%m-%d")

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

    event_ids = [
        get_value(event, 'attributes.user_id'),
        get_value(event, 'client.client_id'),
        get_value(event, 'attributes.device_id')
    ]
    set_value(event, 'attributes.pool_id', next((x for x in event_ids if x),''))




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
    if get_value(event, 'device.locale.country') == 'JO':
        set_value(event, 'attributes.project_id', JORDAN_PROJECT_ID)

    custom_app_mapping(event)
    country = get_value(event,'attributes.country_code')
    if country not in ['', None]:
        set_value(event, 'attributes.country_code', country.upper())


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

def map_and_upload(obj):

    events = []
    file = s3_.Object(IN_BUCKET, obj.key).get()['Body'].read()
    if is_gzip(file):
        file = gzip.decompress(file)
    for line in file.decode().split('\n'):
        if line:
            event = json.loads(line)
            set_value(event, 'attributes.obj_key', get_out_filename(obj.key))

            events.append(json.dumps(map_event(event)))
    out_key = OUT_PREFIX + '/EVENT_LOG_LIST/' + get_out_filename(obj.key)
    s3_.Object(OUT_BUCKET, out_key).put(Body='\n'.join(events))


def save_versions_and_events(ve, obj):
    print(ve.keys())
    for i in ve.keys():
        out_key = OUT_PREFIX + '/v_' + i + '/EVENT_LOG_LIST/'+ get_out_filename(obj.key)
        print('version : '+ i)
        print('saving to : '+out_key)

        s3_.Object(OUT_BUCKET, out_key).put(Body='\n'.join(ve[i]))

def log_file_in_date_range(file_path):
    """
    The method searches for datetime in the file path.
    Schemas that are captured are the form yyyy/mm/dd/hh

    Parameters
    ----------
    file_path : str
        File path or s3 bucket key pointing to log file.

    Returns
    ----------
    True
        If pattern was found in file and date in range
    """
    filename = file_path
    match = re.search(r'(\d{4}/\d{2}/\d{2})', filename)
    date = datetime.strptime(match.group(), '%Y/%m/%d').date()

    return RANGE_FROM <= date <= RANGE_TO

def get_date_range(start, end):
    r = (end + timedelta(days=1) - start).days
    return [start + timedelta(days=i) for i in range(r)]



def preprocess_and_move_logs():
    all_keys = []
    date_range = get_date_range(RANGE_FROM,RANGE_TO)
    input_bucket = s3_.Bucket(IN_BUCKET)

    for folder in IN_S3_FOLDERS:
        for date in date_range:
            formated_date = date.strftime('%Y/%m/%d')
            #_prefix = IN_PREFIX + '/' + folder+'/'+formated_date
            _prefix ='/'.join([IN_PREFIX,folder,formated_date])
            for obj in input_bucket.objects.filter(Prefix=_prefix):

                if obj.key.endswith(IN_SUFFIX): #and has_correct_version(obj):
                    print(obj.key)
                    all_keys.append(obj)

    pool = Pool(4)
    pool.map(map_and_upload, all_keys)

def logs_exist(bucket, prefix='', suffix=''):

    my_bucket = s3_.Bucket(IN_BUCKET)
    date_range = get_date_range(RANGE_FROM, RANGE_TO)
    found = False
    for folder in IN_S3_FOLDERS:
        for date in date_range:
            formated_date = date.strftime('%Y/%m/%d')
            _prefix = prefix +'/'+ folder+'/'+formated_date
            print('Searching in '+_prefix +'\n')
            for obj in my_bucket.objects.filter(Prefix = _prefix):

                if obj.key.endswith(suffix): #and has_correct_version(obj):
                    print('Found ' +obj.key +'\n')
                    found = True
                    break
            if found == True:
                break  #stop searching
        if found == True:
            break
    return found

if logs_exist(bucket=IN_BUCKET, prefix=IN_PREFIX, suffix=IN_SUFFIX):
    print ('Bucket {:s} {:s} prefix contains new {:s} files'.format(
        IN_BUCKET, IN_PREFIX, IN_SUFFIX))

    preprocess_and_move_logs()
else:
    sys.exit(1)