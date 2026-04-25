import google.generativeai as genai
from config import GOOGLE_API_KEY


def list_available_models():
    try:
        genai.configure(api_key=GOOGLE_API_KEY)

        print("🔄 Connecting to Google AI...")
        print("📋 Available Models for your API Key:")
        print("-" * 40)

        found = False
        for m in genai.list_models():
            # Hum sirf wo models dhund rahe hain jo content generate kar sakein
            if 'generateContent' in m.supported_generation_methods:
                print(f"✅ Name: {m.name}")
                found = True

        if not found:
            print("❌ No models found. Please check if API Key is valid.")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    list_available_models()