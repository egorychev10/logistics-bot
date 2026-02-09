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
from aiohttp import web

# Настройки
TOKEN = os.getenv("BOT_TOKEN")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Производственная, 1")

bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# --- Мини-сервер для Render ---
async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- ФИНАЛЬНАЯ ЛОГИКА ОЧИСТКИ АДРЕСА ---
def clean_address(text):
    # 1. Извлекаем блок адреса из ТОРГ-12
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    
    raw = match.group(1).replace('\n', ' ').strip()
    
    # 2. Список стоп-слов (если часть строки содержит это — удаляем часть целиком)
    stop_parts = [
        'р/с', 'к/с', 'бик', 'инн', 'кпп', 'банк', 'ао ', 'пао ', 'ооо ', 'ип ', 
        'общество', 'филиал', 'расчетный', 'корреспондентский',
        'ростокино', 'головинский', 'академический', 'басманный', 'даниловский', # районы
        'округ', 'территория', 'вн.тер.г', 'муниципальный'
    ]

    # 3. Разбиваем на части по запятой и фильтруем
    parts = raw.split(',')
    valid_parts = []
    
    for p in parts:
        p_low = p.lower().strip()
        # Пропускаем пустые или мусорные части
        if not p_low or any(stop in p_low for stop in stop_parts):
            continue
        # Пропускаем части, где только цифры (индексы или счета)
        if re.search(r'\d{10,}', p_low):
            continue
        
        # Чистим г., город и т.д. в конкретной части
        p_clean = re.sub(r'^(г\.|г\s|город|москва)\s*', '', p.strip(), flags=re.IGNORECASE)
        if p_clean:
            valid_parts.append(p_clean.strip())

    # 4. Склеиваем обратно для финальной обработки
    temp_addr = ", ".join(valid_parts)

    # Стандартизируем "ул."
    temp_addr = re.sub(r'\bул\b(?!\.)', 'ул.', temp_addr, flags=re.IGNORECASE)

    # 5. КРАСИВЫЙ НОМЕР ДОМА И КОРПУСА (23, к1)
    # Убираем "д." "дом"
    temp_addr = re.sub(r',\s*(?:д\.|дом)\s*', ', ', temp_addr, flags=re.IGNORECASE)
    # Превращаем " 23, к1" или " 23 к. 1" в " 23к1"
    temp_addr = re.sub(r'(\d+)\s*,\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', temp_addr, flags=re.IGNORECASE)
    temp_addr = re.sub(r'(\d+)\s+(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', temp_addr, flags=re.IGNORECASE)
    
    # Убираем "стр." и лишнее в конце
    temp_addr = re.sub(r'\s*стр\.\s*', ', стр. ', temp_addr, flags=re.IGNORECASE)
    
    # Удаляем висящие знаки препинания и одинокие буквы "г" в конце
    temp_addr = re.sub(r'\s+[гГ]\.?$', '', temp_addr).strip(' ,.')

    return f"Москва, {temp_addr}" if temp_addr else None

# --- Геокодирование и Логистика ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistics_fix_v5")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Пришли PDF. Теперь я фильтрую банки и районы!", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'): return
    
    uid = str(uuid.uuid4())
    temp_fn = f"temp_{uid}.pdf"
    try:
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, temp_fn)
        with pdfplumber.open(temp_fn) as pdf:
            text = "".join([p.extract_text() or "" for p in pdf.pages])
            addr = clean_address(text)
            if addr:
                if message.from_user.id not in user_data: user_data[message.from_user.id] = {'addresses': []}
                user_data[message.from_user.id]['addresses'].append(addr)
                await message.answer(f"✅ **Адрес:**\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❓ Не нашел адрес в {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Сначала пришли PDF!"); return
    
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Адресов: {len(user_data[u_id]['addresses'])}. Водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("🔄 Геокодирую и распределяю...")
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: # Проба без корпуса для поиска
            coords = get_coords(addr.split('к')[0])
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1)

    if not data:
        await message.answer("Адреса не найдены на карте."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        result = f"🚛 **МАРШРУТ ВОДИТЕЛЯ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            final_view = row['address'].replace("Москва, ", "")
            result += f"📍 {final_view}\n"
        await message.answer(result, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
