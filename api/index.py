from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import Dispatcher
import os

# अपनी bot.py से हैंडलर्स इम्पोर्ट करें
from bot import b2b_handlers

app = Flask(__name__)

# Environment Variables से टोकन लें (हम इसे Vercel में सेट करेंगे)
TOKEN = os.environ.get("TELEGRAM_TOKEN")
bot = Bot(token=TOKEN)

# Dispatcher बनाएँ (यहाँ workers=0 ज़रूरी है क्योंकि Serverless में थ्रेड्स काम नहीं करते)
dispatcher = Dispatcher(bot, None, workers=0)

# अपने सारे हैंडलर्स यहाँ रजिस्टर करें
for handler in b2b_handlers:
    dispatcher.add_handler(handler)

@app.route('/', methods=['GET'])
def home():
    return "SAP B2B Bot is Running on Vercel!"

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == "POST":
        # Telegram से आया JSON पढ़ें
        update = Update.de_json(request.get_json(force=True), bot)
        # Dispatcher को भेजें
        dispatcher.process_update(update)
        return "OK", 200

if __name__ == '__main__':
    app.run(debug=True)