import sys
import os

# Python को बता रहे हैं कि बाहर वाले फोल्डर (root) की फाइल्स भी पढ़ें
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import setuptools  # Vercel के pkg_resources एरर को हमेशा के लिए फिक्स करने की मास्टर-की

from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import Dispatcher

# आपके bot.py से मेन क्लास और config से टोकन इम्पोर्ट कर रहे हैं
from config import SALES_BOT_TOKEN
from bot import SAPSalesBot

app = Flask(__name__)

# Telegram Bot और Dispatcher सेटअप (Serverless के लिए workers=0 ज़रूरी है)
bot = Bot(token=SALES_BOT_TOKEN)
dispatcher = Dispatcher(bot, None, workers=0)

# आपके SAP बॉट को स्टार्ट करके सारे हैंडलर्स एक ही बार में रजिस्टर कर रहे हैं
sap_bot_app = SAPSalesBot()
sap_bot_app.setup_dispatcher(dispatcher)

@app.route('/', methods=['GET'])
def home():
    return "SAP B2B Bot is Running on Vercel! 🚀"

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == "POST":
        # Telegram से आया डेटा पढ़ें और बॉट को प्रोसेस करने दें
        update = Update.de_json(request.get_json(force=True), bot)
        dispatcher.process_update(update)
        return "OK", 200

if __name__ == '__main__':
    app.run(debug=True)