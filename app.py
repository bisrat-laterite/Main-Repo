from flask import Flask, request, jsonify
import requests
import gspread
import pandas as pd
import os
import base64
from google.auth.exceptions import GoogleAuthError
import time
# from googleapiclient.errors import HttpError

app = Flask(__name__)

# Replace with your bot's token from BotFather
TELEGRAM_BOT_TOKEN = 'YOUR_BOT_TOKEN_HERE'
TELEGRAM_API_URL = 'https://api.telegram.org/bot6081280787:AAF3HKZAORELluBhj0A90cv62QAWd8ex_Hw/'
main_sheet_key='1kq0JxL3PxB4yxZfBv_2_WOEHvy6kptP7jqB31v0XZoU'
main_sheet_name='project_database'

google_credentials = os.getenv("GOOGLE_CREDENTIALS_BASE64")

def read_gsheet(key, sheet):
    if google_credentials:
        credentials_json = base64.b64decode(google_credentials)
        with open("creds.json", "wb") as f:
            f.write(credentials_json)
    gc=gspread.service_account(filename='creds.json')
    key_=key ### change to the sheets for regron
    ### Reading in the specific googles sheets file
    sh=gc.open_by_key(key_)
    sheet=sheet
    gs=sh.worksheet(sheet)
    #update_id = gs.cell(1, 2).value
    return gs

# def exponential_backoff_request(func, *args, **kwargs):
#     max_attempts = 5
#     for attempt in range(max_attempts):
#         try:
#             return func(*args, **kwargs)
#         except (GoogleAuthError, HttpError) as e:
#             if e.resp.status in [403, 429]:  # Quota or rate limit errors
#                 wait_time = 2 ** attempt  # Exponential backoff
#                 print(f"Rate limit hit, retrying in {wait_time} seconds...")
#                 time.sleep(wait_time)
#             else:
#                 raise
#     raise Exception("Max retries exceeded")

# def append_to_sheet(worksheet, row_data):
#     exponential_backoff_request(worksheet.append_row, row_data)

def send_message(chat_id, text):
    """Send a message to a user."""
    url = TELEGRAM_API_URL + 'sendMessage'
    payload = {'chat_id': chat_id, 'text': text}
    requests.post(url, json=payload)

##function to send the message of the data quality questions 
def send_message_main(chat_id,text):
  base_url=TELEGRAM_API_URL + 'sendMessage'
  parameters={
        'chat_id':chat_id,
        'text':text,
        'parse_mode':'HTML',
        'disable_web_page_preview':True

  }
  success=requests.get(base_url, data=parameters)
  return success.status_code

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming updates from Telegram."""
    update = request.json
    ###second part
    if 'message' in update:
        chat_id = update['message']['chat']['id']
        text = update['message'].get('text', '')

    if text.startswith('/'):
        # command, *args = text.split()
        command=text.split(" ")[0]
        command, *args = text.split(" ")
        if command == '/start':
            send_message(chat_id, "Welcome! Use /help to see available commands.")

        elif command == '/dq':
            if len(text.split(" "))==2:
                args=text.split(" ")[1]
                main=read_gsheet(main_sheet_key, main_sheet_name)
                main_content=pd.DataFrame(main.get_all_records())
                key=list(main_content[main_content['project_id']==args]['key'])[0]
                # project_key=project_link.replace('//', '/').split('/')[4]
                try:
                    read_st=pd.DataFrame(read_gsheet(key, "Data_Quality").get_all_records()).iloc[2,1]
                    send_message(chat_id, str(read_st))
                except:
                    send_message(chat_id, "Some errors_ let the project manager know")
    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000)