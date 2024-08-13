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

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming updates from Telegram."""
    update = request.json
    ###second part
    if 'message' in update:
        chat_id = update['message']['chat']['id']
        text = update['message'].get('text', '')

    if text.startswith('/'):
        command, *args = text.split()
        #command=text.split(" ")[0]
        # command, *args = text.split(" ")
        if command == '/start':
            send_message(chat_id, "Welcome! Use /help to see available commands.")
        elif command == '/dq':
            send_message(chat_id, "/start - Welcome message\n/help - List commands\n/echo [text] - Echo back text")
            key='1kq0JxL3PxB4yxZfBv_2_WOEHvy6kptP7jqB31v0XZoU'
            sheet_name="project_database"
            gs_=read_gsheet(key, sheet_name)
            update_id = gs_.cell(1, 2).value
            send_message(chat_id, update_id)

            # _all=gs_.get_all_records()
            # # working on the gsheets returned
            # dataframe = pd.DataFrame(_all)
            # filter1= dataframe['project_id']==arg
            # val=list(dataframe[filter1]['project_key'])[0]
            # send_message(chat_id,str(val))
            #project_id
    # if 'message' in update:
    #     chat_id = update['message']['chat']['id']
    #     text = update['message'].get('text', '')
    

    #     # Handle the message and respond
    #     if text == '/start':
    #         send_message(chat_id, "Welcome! How can I help you today?")
    #         send_message(chat_id, str(ID))
    #         #append_to_sheet
    #     elif text == '/help':
    #         send_message(chat_id, "Here are the commands you can use...")
    #     else:
    #         send_message(chat_id, f"You said: {text}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000)