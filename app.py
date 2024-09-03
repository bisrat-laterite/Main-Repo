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


def str_to_dict(string):
    ### function to identify the data quality question being responded to 
    # remove the curly braces from the string
    string = string.strip(' ').replace('Data Quality Bot', 'Data Quality Bot:None')
    # split the string into key-value pairs
    pairs = string.split('\n')
    # print(pairs)
    pre= {key[0].rstrip().lstrip():key[1].rstrip().lstrip() for key in (pair.split(':') for pair in pairs) if key[0].rstrip().lstrip() in ['HHID', 'Variable', 'FC Name', 'Project ID', 'Task']}
    # print(pre)
    return pre

def getting_responses(gs,main_text, text, column):
    """getting responses from the enumerator"""
    find_key=main_text['HHID']
    find_variable=main_text['Variable']
    # finding hhid
    hhid=[]
    # print(text)
    # the filter to be abstracted away
    [hhid.append(l.row) for l in gs.findall(find_key)]
    # finding variable
    variable=[]
    # the filter to be abstracted away
    [variable.append(l.row) for l in gs.findall(find_variable)]
    row=list(set(hhid).intersection(variable))
    print(row)
    for r in row:
        # val = gs.cell(r, 11).value
        # print(val)
        # if val== None:
        gs.update_cell(r, column, text)

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
        ### handling requests
    if 'message' in update:
        chat_id = update['message']['chat']['id']
        text = update['message'].get('text', '')

        if 'reply_to_message' not in update['message']:
            if text.startswith('/'):
                # command, *args = text.split()
                command=text.split(" ")[0]
                command, *args = text.split(" ")
                if command == '/start':
                    send_message(chat_id, "Welcome! Use /help to see available commands.")
                ###  using the /dq command
                elif command == '/dq':
                    if len(text.split(" "))==2:
                        args=text.split(" ")[1]
                        main=read_gsheet(main_sheet_key, main_sheet_name)
                        main_content=pd.DataFrame(main.get_all_records())
                        ### project key
                        ### Checking 
                        if args in list(main_content['project_id']):
                            key=list(main_content[main_content['project_id']==args]['key'])[0]
                            ### project manager
                            manager=list(main_content[main_content['project_id']==args]['manager'])[0]
                            ### if key not found send an error message
                            # project_key=project_link.replace('//', '/').split('/')[4]
                            # send_message(chat_id, key)
                            try:
                                a=read_gsheet(key, "Data Quality - General")
                                content=pd.DataFrame(a.get_all_records())
                                filtered=content[content['chat_id']==chat_id]
                                ### send only pending/ clarification needed comments
                                filtered=filtered[filtered['Status'].isin(["Pending", "Clarification Needed"])]
                                filtered=filtered[filtered['Enumerator Response']==""]
                                for index, row in filtered.iterrows():
                                    text=(str(dict(row)))
                                    text =  "<a href='https://www.laterite.com/'>Data Quality Bot</a>" \
                                    + "\n" + f"<b>Enumerator Name: </b>"+ row['Enumerator'] + \
                                        "\n" +   f"<b>HHID: </b>" + str(row['HHID'])  + \
                                        "\n" +   f"<b>Variable: </b>" + row['Variable'] \
                                        +  "\n" +   f"<b>Data Quality Question :</b>" + row['issue_description'] \
                                        + "\n" + f"<b>Task :</b> Data quality" \
                                    + "\n" +  f"<b>Project ID: </b> "+ args
                                    send_message_main(chat_id, text)
                                # send_message(chat_id, "success")
                            except:
                                send_message(chat_id, f"Some error let the project manager ({manager}/Bisrat) know")
                        else:
                            send_message(chat_id, f"the project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"the command /dq takes one argument(only one) eg. /dq wb_tst_1, Please try again with the correct format!")
                ### translation sheet
                elif command == '/tr':
                    if len(text.split(" "))==2:
                        args=text.split(" ")[1]
                        main=read_gsheet(main_sheet_key, main_sheet_name)
                        main_content=pd.DataFrame(main.get_all_records())
                        ### project key
                        ### Checking 
                        if args in list(main_content['project_id']):
                            key=list(main_content[main_content['project_id']==args]['key'])[0]
                            ### project manager
                            manager=list(main_content[main_content['project_id']==args]['manager'])[0]
                            ### if key not found send an error message
                            # project_key=project_link.replace('//', '/').split('/')[4]
                            # send_message(chat_id, key)
                            try:
                                a=read_gsheet(key, "Data Quality - Translations")
                                content=pd.DataFrame(a.get_all_records())
                                filtered=content[content['enum_chat']==chat_id]
                                ### send only pending/ clarification needed comments
                                filtered=filtered[filtered['TASK_STATUS'].isin(["Pending", "Clarification Needed"])]
                                filtered=filtered[filtered['Field_Response']==""]
                                for index, row in filtered.iterrows():
                                    text=(str(dict(row)))
                                    text =  "<a href='https://www.laterite.com/'>Data Quality Bot</a>" \
                                    + "\n" + f"<b>Enumerator Name: </b>"+ row['enum_name'] + \
                                        "\n" +   f"<b>HHID: </b>" + str(row['HHID'])  + \
                                        "\n" +   f"<b>Variable: </b>" + row['Variable'] \
                                        +  "\n" +   f"<b>Translation Item :</b>" + row['item_to_translate'] \
                                        + "\n" + f"<b>Task :</b> Translation" \
                                    + "\n" +  f"<b>Project ID: </b> "+ args                      
                                    send_message_main(chat_id, text)
                                # send_message(chat_id, "success")
                            except:
                                send_message(chat_id, f"Some error let the project manager ({manager}/Bisrat) know")
                        else:
                            send_message(chat_id, f"the project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"the command /tr takes one argument(only one) eg. /tr wb_tst_1, Please try again with the correct format!")
    
        if 'reply_to_message' in update['message']:   
        # handling responses
            pre_message_inf=update['message']['reply_to_message']
            message=update['message'] if 'message' in update else update['edited_message'] if "edited_message" in update else ""
            #### getting a dict of the text send
            pre_message=str_to_dict(pre_message_inf['text'])
            ### editing the main sheet
            if 'text' in message.keys():
                reply_text=message['text']
                ### retrieve project_id
                project_id=pre_message['Project ID']
                main=read_gsheet(main_sheet_key, main_sheet_name)
                main_content=pd.DataFrame(main.get_all_records())
                key=list(main_content[main_content['project_id']==project_id]['key'])[0]

                ### identifying which sheet to edit
                if pre_message['Task']=="Translation":
                    name_sheet="Data Quality - Translations"
                    row_cell=12
                elif pre_message['Task']=="Data quality":
                    name_sheet="Data Quality - General"
                    row_cell=11
                else:
                    return None
                ### reading the gsheet
                gs=read_gsheet(key, name_sheet)
                ### updating the sheet
                getting_responses(gs, pre_message, reply_text, row_cell)
            else:
                ### if enumerator did not respond in the right format send message notifiying
                send_message(chat_id, "Please respond in a written format. Thank you!")

                    
    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000)