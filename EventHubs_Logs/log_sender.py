
import sys, os, re, gzip, json, urllib.parse, urllib.request, traceback, datetime, calendar, logging
import azure.functions as func
from base64 import b64decode


logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

s247_datetime_format_string = logtype_config['dateFormat']

if 'unix' not in s247_datetime_format_string:
    is_year_present = True if '%y' in s247_datetime_format_string or '%Y' in s247_datetime_format_string else False
    if is_year_present is False:
        s247_datetime_format_string = s247_datetime_format_string+ ' %Y'

def get_timestamp(datetime_string):
    try:
        datetime_data = datetime.datetime.strptime(datetime_string, s247_datetime_format_string)
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

def get_json_value(obj, key):
    if key in obj:
        return obj[key]
    elif '.' in key:
        parent_key = key[:key.index('.')]
        child_key = key[key.index('.')+1:]
        return get_json_value(obj[parent_key], child_key)

def json_log_parser(lines_read):
    log_size = 0;
    parsed_lines = []
    for event_obj in lines_read:
        formatted_line = {}
        for path_obj in logtype_config['jsonPath']:
            value = get_json_value(event_obj, path_obj['key' if 'key' in path_obj else 'name'])
            if value:
                formatted_line[path_obj['name']] = value 
                log_size+= len(str(value))
        if not is_filters_matched(formatted_line):
            continue
        formatted_line['_zl_timestamp'] = get_timestamp(formatted_line[logtype_config['dateField']])
        formatted_line['s247agentuid'] = 'resource-group'
        parsed_lines.append(formatted_line)
    return parsed_lines, log_size

def send_logs_to_s247(gzipped_parsed_lines, log_size):
    header_obj = {'X-DeviceKey': logtype_config['apiKey'], 'X-LogType': logtype_config['logType'],
                  'X-StreamMode' :1, 'Log-Size': log_size, 'Content-Type' : 'application/json', 'Content-Encoding' : 'gzip', 'User-Agent' : 'AWS-Lambda'
    }
    upload_url = 'https://'+logtype_config['uploadDomain']+'/upload'
    request = urllib.request.Request(upload_url, headers=header_obj)
    s247_response = urllib.request.urlopen(request, data=gzipped_parsed_lines)
    dict_responseHeaders = dict(s247_response.getheaders())
    if s247_response and s247_response.status == 200:
        print('{}:All logs are uploaded to site24x7'.format(dict_responseHeaders['x-uploadid']))
    else:
        print('{}:Problem in uploading to site24x7 status {}, Reason : {}'.format(dict_responseHeaders['x-uploadid'], s247_response.status, s247_response.read()))

def main(event: func.EventHubEvent):
    logging.info('S247 Function triggered to process a message: %s', event.get_body().decode('utf-8'))
    try:
        payload = json.loads(event.get_body().decode('utf-8'))

        log_events = payload['records']
        if 'jsonPath' in logtype_config:
            parsed_lines, log_size = json_log_parser(log_events)

        if parsed_lines:
            gzipped_parsed_lines = gzip.compress(json.dumps(parsed_lines).encode())
            send_logs_to_s247(gzipped_parsed_lines, log_size)
    except Exception as e:
        print(e)
        raise e
