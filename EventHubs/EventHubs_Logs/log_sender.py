import json

def main(event):
    msg_body = event.get_body()
    msg = json.dumps(msg_body.decode('uft-8'))
    print(msg)
