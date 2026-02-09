import os
import re
import asyncio
import pdfplumber
import pandas as pd
import uuid
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from geopy.geocoders import Nominatim
from sklearn.cluster import KMeans
import requests
from aiohttp import web

# Настройки
TOKEN = os.getenv("BOT_TOKEN")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Производственная, 1")

bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# --- Вспомогательный веб-сервер для Render (чтобы не засыпал) ---
async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- Логика обработки адреса ---
def clean_address(text):
    """Очистка адреса из ТОРГ-12 согласно инструкции"""
    # Ищем текст между ОКПД и Грузополучателем (регистронезависимо)
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    
    if not match:
        return None
    
    addr = match.group(1).strip()
    # Убираем лишние банковские реквизиты, ИНН, КПП, индексы (6 цифр)
    addr = re.sub(r'\d{10,12}', '', addr) # ИНН
    addr = re.sub(r'\b\d{6}\b', '', addr) # Индексы
    addr = re.sub(r'[,]{2,}', ',', addr) # Двойные запятые
    
    # Оставляем только значимую часть адреса
    parts = addr.split(',')
    clean_parts = []
    for p in parts:
        p = p.strip()
        # Пропускаем технические поля (р/с, к/с, БИК)
        if any(x in p.lower() for x in ['р/с', 'к/с', 'бик', 'тел']):
            continue
        if p:
            clean_parts.append(p)
    
    final_addr = ", ".join(clean_parts)
    return "Москва, " + final_addr if "Москва" not in final_addr else final_addr

def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="my_logistic_bot_v2")
        location = geolocator.geocode(address, timeout=10)
        if location:
            return (location.latitude, location.longitude)
    except:
        return None
    return None

# --- Обработчики команд ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Привет! Перешли мне PDF-накладные. После загрузки всех файлов нажми кнопку ниже.", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'):
        return
    
    uid = str(uuid.uuid4())
    temp_filename = f"temp_{uid}.pdf"
    
    try:
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, temp_filename)
        
        with pdfplumber.open(temp_filename) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
            
            addr = clean_address(text)
            if addr:
                if message.from_user.id not in user_data:
                    user_data[message.from_user.id] = {'addresses': []}
                user_data[message.from_user.id]['addresses'].append(addr)
                await message.answer(f"✅ Адрес добавлен: {addr}")
            else:
                await message.answer(f"⚠️ Не нашел адрес в {message.document.file_name}")
    except Exception as e:
        print(f"Error: {e}")
        await message.answer(f"❌ Ошибка в файле {message.document.file_name}")
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Сначала пришли мне PDF файлы!")
        return
    
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Всего адресов: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("⏳ Строю оптимальные маршруты (это займет время)...")
    
    data = []
    for addr in addresses:
        coords = get_coords(addr)
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1) # Соблюдаем лимит Nominatim

    if not data:
        await message.answer("Не удалось найти координаты. Проверьте формат накладных.")
        return

    df = pd.DataFrame(data)
    n_clusters = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_clusters, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    for i in range(n_clusters):
        driver_points = df[df['driver'] == i]
        result = f"📋 **ВОДИТЕЛЬ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            # Очистка для водителя (убираем город)
            short_addr = row['address'].replace("Москва, ", "").replace("г. Москва, ", "")
            result += f"📍 {short_addr}\n"
        await message.answer(result)

    user_data[user_id] = {'addresses': []}

# --- Запуск ---
async def main():
    # Запускаем веб-сервер и бота одновременно
    await asyncio.gather(
        start_web_server(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    asyncio.run(main())
