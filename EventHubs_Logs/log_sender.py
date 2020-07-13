
import sys, os, re, gzip, json, urllib.parse, urllib.request, traceback, datetime, calendar, logging
import azure.functions as func
from base64 import b64decode

logtype_config = None
s247_datetime_format_string = None

def get_timestamp(datetime_string):
    try:
        datetime_data = datetime.datetime.strptime(datetime_string[:26], s247_datetime_format_string)
        timestamp = calendar.timegm(datetime_data.utctimetuple()) *1000 + int(datetime_data.microsecond/1000)
        return int(timestamp)
    except Exception as e:
        return 0

def is_filters_matched(formatted_line):
    if 'filterConfig' in logtype_config:
        for config in logtype_config['filterConfig']:
            if config in formatted_line and (filter_config[config]['match'] ^ (formatted_line[config] in filter_config[config]['values'])):
                return False
    return True

def get_json_value(obj, key, type=None):
    if key in obj:
        if type and type == 'json-object':
            arr_json = []
            for child_key in obj[key]:
                arr_json.append({'key' : child_key, 'value': str(obj[key][child_key])})
            return arr_json
        else:
            return obj[key]
    elif '.' in key:
        parent_key = key[:key.index('.')]
        child_key = key[key.index('.')+1:]
        return get_json_value(obj[parent_key], child_key)

def json_log_parser(lines_read):
    log_size = 0
    parsed_lines = []
    for event_obj in lines_read:
        try:
            formatted_line = {}
            for path_obj in logtype_config['jsonPath']:
                value = get_json_value(event_obj, path_obj['key' if 'key' in path_obj else 'name'], path_obj['type'] if 'type' in path_obj else None)
                if value:
                    formatted_line[path_obj['name']] = value 
                    log_size+= len(str(value))
            if not is_filters_matched(formatted_line):
                continue
            formatted_line['_zl_timestamp'] = get_timestamp(formatted_line[logtype_config['dateField']])
            if 'resourceId' in event_obj:
                formatted_line['s247agentuid'] = event_obj['resourceId'].split('/')[4]
            parsed_lines.append(formatted_line)
        except Exception as e:
            print('unable to parse event message : '+str(e))
            pass
    return parsed_lines, log_size

def send_logs_to_s247(gzipped_parsed_lines, log_size):
    header_obj = {'X-DeviceKey': logtype_config['apiKey'], 'X-LogType': logtype_config['logType'],
                  'X-StreamMode' :1, 'Log-Size': log_size, 'Content-Type' : 'application/json', 'Content-Encoding' : 'gzip', 'User-Agent' : 'AZURE-Function'
    }
    upload_url = 'https://'+logtype_config['uploadDomain']+'/upload'
    request = urllib.request.Request(upload_url, headers=header_obj)
    s247_response = urllib.request.urlopen(request, data=gzipped_parsed_lines)
    dict_responseHeaders = dict(s247_response.getheaders())
    if s247_response and s247_response.status == 200:
        logging.info('%s :All logs are uploaded to site24x7', dict_responseHeaders['x-uploadid'])
    else:
        logging.info('%s :Problem in uploading to site24x7 status %s, Reason : %s', dict_responseHeaders['x-uploadid'], s247_response.status, s247_response.read())

def main(eventMessages: func.EventHubEvent):
    try:
        global logtype_config, s247_datetime_format_string
        cardinality = 'many'
        if type(eventMessages) != list:
            eventMessages = [eventMessages]
            cardinality = 'one'
        for eventMessage in eventMessages:
            payload = json.loads(eventMessage.get_body().decode('utf-8'))
            log_events = payload['records'] if cardinality == 'many' else payload[0]['records'] 
            log_category = (log_events[0]['category']).replace('-', '_')
            print("log_category" + " : "+ log_category)
            log_category = 'S247_'+log_category
            if log_category in os.environ:
                print("log_category found in input arguments")
                logtype_config = json.loads(b64decode(os.environ[log_category]).decode('utf-8'))
                s247_datetime_format_string = logtype_config['dateFormat']
            else:
                logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))
                s247_datetime_format_string = logtype_config['dateFormat']
    
    
            if 'jsonPath' in logtype_config:
                parsed_lines, log_size = json_log_parser(log_events)
    
            if parsed_lines:
                gzipped_parsed_lines = gzip.compress(json.dumps(parsed_lines).encode())
                send_logs_to_s247(gzipped_parsed_lines, log_size)
    except Exception as e:
        traceback.print_exc()
        raise e
