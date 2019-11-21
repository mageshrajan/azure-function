import json, logging
import azure.functions as func

def main(event: func.EventHubEvent):
    logging.info('S247 Function triggered to process a message: %s', event.get_body().decode('utf-8'))