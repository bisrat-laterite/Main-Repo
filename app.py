from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Replace with your bot's token from BotFather
TELEGRAM_BOT_TOKEN = 'YOUR_BOT_TOKEN_HERE'
TELEGRAM_API_URL = 'https://api.telegram.org/bot6081280787:AAF3HKZAORELluBhj0A90cv62QAWd8ex_Hw'

def send_message(chat_id, text):
    """Send a message to a user."""
    url = TELEGRAM_API_URL + 'sendMessage'
    payload = {'chat_id': chat_id, 'text': text}
    requests.post(url, json=payload)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming updates from Telegram."""
    update = request.json

    if 'message' in update:
        chat_id = update['message']['chat']['id']
        text = update['message'].get('text', '')

        # Handle the message and respond
        if text == '/start':
            send_message(chat_id, "Welcome! How can I help you today?")
        elif text == '/help':
            send_message(chat_id, "Here are the commands you can use...")
        else:
            send_message(chat_id, f"You said: {text}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000)