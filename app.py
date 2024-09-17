from flask import Flask, request, jsonify
import requests
import gspread
import pandas as pd
import os
import base64
from google.auth.exceptions import GoogleAuthError
import time
import ast
import re
# from googleapiclient.errors import HttpError

app = Flask(__name__)

# Replace with your bot's token from BotFather
TELEGRAM_BOT_TOKEN = os.getenv("Bot_token")
TELEGRAM_API_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/'
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
def sendpoll(chat_id, options,text):
    """Send a names to a user."""
    url = TELEGRAM_API_URL + 'sendPoll'
    payload = {'chat_id': chat_id, 'question': text, 'options':options, 'is_anonymous':False}
    response=requests.post(url, json=payload)
    return response

def handle_poll_result(poll_answer):
    if poll_answer:
        user_id = poll_answer['user']['id']
        option_ids = poll_answer['option_ids']  # This is a list of selected option indices
        poll_id= poll_answer['poll_id']
        option_ids = option_ids[0] if option_ids else None
        return user_id, option_ids, poll_id


    
def send_message(chat_id, text):
    """Send a message to a user."""
    url = TELEGRAM_API_URL + 'sendMessage'
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode':'HTML'}
    requests.post(url, json=payload)

def send_inline_keyboard(chat_id, options, text):
    """Send an inline keyboard with the matching options."""
    keyboard = [[{"text": option, "callback_data": options.index(option)}] for option in options]

    reply_markup = {
        "inline_keyboard": keyboard
    }

    url = TELEGRAM_API_URL + "sendMessage"
    data = {
        "chat_id": chat_id,
        "reply_markup": reply_markup,
        "text":text,
        'parse_mode':'HTML'
    }
    requests.post(url, json=data)

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
    # update['callback_query']
    poll_answer = update.get('callback_query', '')
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
                            enum_list_pre=read_gsheet(key, "ENUM_LIST")
                            enum_list=list(pd.DataFrame(enum_list_pre.get_all_records())['CHAT_ID'])
                            if chat_id not in enum_list:
                                send_message(chat_id, f"You have not yet registered to the {args} project. Please do so using [/rg {args}] and following the steps accordingly.")
                            else:
                                try:
                                    a=read_gsheet(key, "Data Quality - General")
                                    content=pd.DataFrame(a.get_all_records())
                                    filtered=content[content['chat_id']==chat_id]
                                    ### send only pending/ clarification needed comments
                                    filtered=filtered[filtered['Status'].isin(["Pending", "Clarification Needed"])]
                                    filtered=filtered[filtered['Enumerator Response']==""]
                                    if filtered.shape[0]==0:
                                        send_message(chat_id, "Thank you for all your responses. You have no data quality items remaining under your name")
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
                            send_message(chat_id, f"The project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"The command /dq takes one argument(only one) eg. /dq wb_tst_1, Please try again with the correct format!")
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
                            enum_list_pre=read_gsheet(key, "ENUM_LIST")
                            enum_list=list(pd.DataFrame(enum_list_pre.get_all_records())['CHAT_ID'])
                            if chat_id not in enum_list:
                                send_message(chat_id, f"You have not yet registered to the {args} project. Please do so using [/rg {args}] and following the steps accordingly.")
                            else:
                                try:
                                    a=read_gsheet(key, "Data Quality - Translations")
                                    content=pd.DataFrame(a.get_all_records())
                                    filtered=content[content['enum_chat']==chat_id]
                                    ### send only pending/ clarification needed comments
                                    filtered=filtered[filtered['TASK_STATUS'].isin(["Pending", "Clarification Needed"])]
                                    filtered=filtered[filtered['Field_Response']==""]
                                    if filtered.shape[0]==0:
                                        send_message(chat_id, "Thank you for all your responses. You have no translations remaining under your name")
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
                            send_message(chat_id, f"The project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"The command /tr takes one argument(only one) eg. /tr wb_tst_1, Please try again with the correct format!")
                ### registration sheet
                elif command == '/rg':
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
                                a=read_gsheet(key, "ENUM_LIST")
                                content=pd.DataFrame(a.get_all_records())
                                # filtered=content[content['enum_chat']==chat_id]
                                ### send only pending/ clarification needed comments
                                # content['Name_project']=content['NAME']+" "+f"[{args}]"
                                Names_=list(content['NAME'])
                                chats=list(content['CHAT_ID'])
                                if chat_id not in chats:
                                    text="<a href='https://t.me/bisget_bot'>Data Quality Bot</a>" \
                                    +"\n" + f"Please select your name from the list [{args}]."
                                    send_inline_keyboard(chat_id, Names_, text)
                                    # send_message_options(chat_id, text,keyboard)
                                    # response=sendpoll(chat_id, Names_,text)
                                    # if response.status_code == 200:
                                    #         # Parse the response JSON
                                    #         poll_info = response.json()
                                    #         # Extract the poll_id
                                    #         poll_id = poll_info['result']['poll']['id']
                                    #         gs=read_gsheet(main_sheet_key, "Polling")
                                    #         value=ast.literal_eval(gs.cell(1, 1).value)
                                    #         value[poll_id]=args
                                    #         gs.update_cell(1, 1, str(value))
                                else:
                                    pairs_ = dict(zip(chats, Names_))
                                    send_message(chat_id, f"You have already registered as <b>{pairs_[chat_id]}</b>. Please let {manager} and/or Bisrat know if you are not <b>{pairs_[chat_id]}</b>!")
                            except:
                                send_message(chat_id, f"Some error let the project manager ({manager}/Bisrat) know")
                        else:
                            send_message(chat_id, f"The project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"The command /rg takes one argument(only one) eg. /rg wb_tst_1, Please try again with the correct format!")
                ### registration sheet
                elif command == '/dr':
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
                                a=read_gsheet(key, "Daily_Report")
                                content=pd.DataFrame(a.get_all_records())
                                dates=list(set(list(content['today'])))
                                chat_ids=list(set(list(content['CHAT_ID'])))
                                if chat_id in chat_ids:
                                    result = ", ".join(dates)
                                    send_message_main(chat_id, result)
                                    text="<a href='https://t.me/bisget_bot'>Data Quality Bot</a>" \
                                    +"\n"+ f"Please select the date for which you would like daily report from the list |{args}|."
                                    send_inline_keyboard(chat_id, dates, text)
                                else:
                                    send_message(chat_id, "You are either not part of this project or there are no completed surveys under you name.")
                                # if chat_id not in chats:
                                #     text=f"Please select your name from the list [{args}]."
                                #     send_inline_keyboard(chat_id, Names_, text)
                                #     # send_message_options(chat_id, text,keyboard)
                                #     # response=sendpoll(chat_id, Names_,text)
                                #     # if response.status_code == 200:
                                #     #         # Parse the response JSON
                                #     #         poll_info = response.json()
                                #     #         # Extract the poll_id
                                #     #         poll_id = poll_info['result']['poll']['id']
                                #     #         gs=read_gsheet(main_sheet_key, "Polling")
                                #     #         value=ast.literal_eval(gs.cell(1, 1).value)
                                #     #         value[poll_id]=args
                                #     #         gs.update_cell(1, 1, str(value))
                                # else:
                                #     pairs_ = dict(zip(chats, Names_))
                                #     send_message(chat_id, f"You have already registered as <b>{pairs_[chat_id]}</b>. Please let {manager} and/or Bisrat know if you are not <b>{pairs_[chat_id]}</b>!")
                            except:
                                send_message(chat_id, f"Some error let the project manager ({manager}/Bisrat) know")
                        else:
                            send_message(chat_id, f"The project id you specified({args}) is wrong. Please try again with the right project id.")
                    else:
                        send_message(chat_id, f"The command /dr takes one argument(only one) eg. /dr wb_tst_1, Please try again with the correct format!")
        if 'reply_to_message' in update['message']:   
        # handling responses
            pre_message_inf=update['message']['reply_to_message']
            message=update['message'] if 'message' in update else update['edited_message'] if "edited_message" in update else ""
            #### getting a dict of the text send
            pre_message=str_to_dict(pre_message_inf['text'])
            ### editing the main sheet
            if pre_message!={}:
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
                        # pre_message['Task']=="Data quality"
                        name_sheet="Data Quality - General"
                        row_cell=11
                    else:
                        send_message(chat_id, "Only respond to data quality and translation requests.")
                    ### reading the gsheet
                    gs=read_gsheet(key, name_sheet)
                    ### updating the sheet
                    getting_responses(gs, pre_message, reply_text, row_cell)
                else:
                    send_message(chat_id, "Please respond only in written format. Thank you")
            else:
                ### if enumerator did not respond in the right format send message notifiying
                send_message(chat_id, "Only respond to data quality and translation requests.")

    elif poll_answer!="":
        print(poll_answer.keys())
        print(poll_answer['message'].keys())
        print(poll_answer['message']['chat'].keys())

        user_id=poll_answer['message']['chat']['id']
        first_name=poll_answer['message']['chat']['first_name']
        user_name=poll_answer['message']['chat'].get('username', '')
        print(user_id, first_name, user_name)
        # # continue

        option=poll_answer['data']
        # user_id=poll_answer['message']['chat_id']
        poll_id=poll_answer['message']['text']
        # text=poll_answer[]
        # callback_query.message.text
        # send_message(user_id, "ok")
        pattern = r'\[([^\[\]]*)\]'
        match = re.search(pattern, poll_id)

        pattern2= r'\|([^\[\]]*)\|'
        match2= re.search(pattern2, poll_id)
        if match:
            project_id=match.group(1)
            # send_message(user_id, f"thanks, project is {project_id}")
        #     ### reading the main sheet
            main=read_gsheet(main_sheet_key, main_sheet_name)
            main_content=pd.DataFrame(main.get_all_records())
            key=list(main_content[main_content['project_id']==project_id]['key'])[0]
            manager=list(main_content[main_content['project_id']==project_id]['manager'])[0]
            print(key)
            # send_message(user_id, f"success option is {option}, text is {poll_id}")
            try:
                enum=read_gsheet(key, "ENUM_LIST")
                enum_df=pd.DataFrame(enum.get_all_records())
                chat_ids=list(enum_df['CHAT_ID'])
                first_names=list(enum_df['FIRST_NAME'])
                Usernames=list(enum_df['USER_NAME'])
                Namez=list(enum_df['NAME'].astype(str))
                dict_=dict(zip(Namez, chat_ids))
                dict2_=dict(zip(Namez, first_names))
                dict3_=dict(zip(Namez, Usernames))
                # chat_id_alredy=dict_[Namez[option]]
                print(Namez)
                print(dict_)
                print(option)
                name=Namez[int(option)]
                print(dict_[name], "check")
                if dict_[name]!='':
                    send_message(user_id, f"Someone[{dict2_[name], dict3_[name]}] already has already registered as {name}. If you are not that person, please let {manager}/Bisrat know.")
                else:
                    #send_message(user_id, f"You are registering as {name} if this is not correct contact {manager} and/or Bisrat")
                    enum2=read_gsheet(key, "ENUM_LIST")
                    enum2.update_cell(int(option)+2, 3, str(user_id))
                    enum2.update_cell(int(option)+2, 4, str(first_name))
                    enum2.update_cell(int(option)+2, 5, str(user_name))

            except:
                send_message(user_id, f"Some error please contact bisrat!")
            ### updating the list based on the 
            # send_message(user_id, f"you selected {option} for poll id {poll_id}")
        elif match2:
            project_id=match2.group(1)
            # send_message(user_id, f"thanks, project is {project_id}")
        #     ### reading the main sheet
            main=read_gsheet(main_sheet_key, main_sheet_name)
            main_content=pd.DataFrame(main.get_all_records())
            key=list(main_content[main_content['project_id']==project_id]['key'])[0]
            manager=list(main_content[main_content['project_id']==project_id]['manager'])[0]
            print(key)
            try:
                daily_report=pd.DataFrame(read_gsheet(key, "Daily_Report").get_all_records())
                # daily_report=daily_report[daily_report['CHAT_ID']==user_id]
                send_message(user_id, "works till this point.")
                dates=list(set(list(daily_report['today'])))
                print(dates)
                print(option)
                print(dates[int(option)])
                # dates=list(set(dates))
                #ok dkjdfj
                # send_message(user_id, "works till this point."+str(dates[option]))
                hhids=daily_report[daily_report['today']==dates[int(option)]][['hhid', 'CHAT_ID']]
                hhids=hhids[hhids['CHAT_ID']==user_id]['hhid']
                print(hhids.head())
                print(list(hhids))

                ids="\n".join([str(x) for x in hhids])
                # print(ids)
                send_message(user_id,f"you have completed these households on {str(dates[int(option)])} \n {ids}")
            except:
                send_message(user_id, f"Some error please contact bisrat!")

        else:
            send_message(user_id, "some error project id not found")

    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000)