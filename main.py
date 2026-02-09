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

# Настройки из переменных окружения
TOKEN = os.getenv("BOT_TOKEN")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Производственная, 1")

bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# --- Мини-сервер для Render ---
async def handle_health(request):
    return web.Response(text="Bot is alive")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- Улучшенная очистка адреса ---
def clean_address(text):
    # 1. Вырезаем блок между ключевыми фразами
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        # Если не нашли по ОКПД, пробуем найти после слова 'Грузополучатель'
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер документа)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match:
        return None

    raw_addr = match.group(1).replace('\n', ' ').strip()
    
    # Список "мусора", который нужно выкинуть
    junk_words = [
        'ИНН', 'КПП', 'вн.тер.г.', 'муниципальный округ', 'административный округ', 
        'р/с', 'к/с', 'БИК', 'тел.', 'бизнес-центр', 'БЦ', 'этаж', 'офис', 'помещение'
    ]
    
    # Признаки того, что часть строки является адресом
    address_markers = ['ул', 'д.', 'стр', 'корп', 'пр-т', 'проспект', 'пер', 'проезд', 'шоссе', 'наб', 'тупик']

    # Разбиваем по запятым и фильтруем
    parts = raw_addr.split(',')
    clean_parts = []
    
    for p in parts:
        p_clean = p.strip()
        # Пропускаем, если в части есть ИНН или р/с (длинные цифры)
        if re.search(r'\d{10,}', p_clean):
            continue
        # Пропускаем пустые или слишком короткие части (например, точки или КПП)
        if len(p_clean) < 2 or p_clean.lower() in ['инн', 'кпп']:
            continue
        # Убираем конкретные мусорные фразы
        for junk in junk_words:
            p_clean = re.sub(rf'{junk}.*?\s', '', p_clean, flags=re.IGNORECASE).strip()
            p_clean = p_clean.replace(junk, "").strip()

        # Оставляем только те части, где есть "Москва" или маркеры улицы/дома
        if "москва" in p_clean.lower() or any(m in p_clean.lower() for m in address_markers):
            # Финальная чистка от лишних знаков в начале/конце части
            p_clean = re.sub(r'^[^а-яА-Я0-9]+|[^а-яА-Я0-9]+$', '', p_clean)
            if p_clean:
                clean_parts.append(p_clean)

    if not clean_parts:
        return None

    # Собираем обратно. Гарантируем, что "Москва" в начале.
    final = ", ".join(clean_parts)
    if "Москва" not in final:
        final = "Москва, " + final
        
    return final

def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistics_bot_v3")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except:
        return None

# --- Обработчики ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Сбрасывай PDF-накладные. Я очищу адреса от мусора и построю маршрут.", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'):
        return
    
    uid = str(uuid.uuid4())
    temp_fn = f"temp_{uid}.pdf"
    
    try:
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, temp_fn)
        
        with pdfplumber.open(temp_fn) as pdf:
            text = "".join([p.extract_text() or "" for p in pdf.pages])
            addr = clean_address(text)
            
            if addr:
                if message.from_user.id not in user_data:
                    user_data[message.from_user.id] = {'addresses': []}
                user_data[message.from_user.id]['addresses'].append(addr)
                await message.answer(f"📍 Очищенный адрес:\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❓ Не удалось вычленить адрес из {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn):
            os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Нет данных. Пришли PDF!")
        return
    
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено адресов: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    # Очистка дублей
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("🔄 Геокодирую и считаю маршруты...")
    
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1) # Лимит Nominatim

    if len(data) < num_drivers:
        await message.answer(f"Слишком мало найденных адресов ({len(data)}) для {num_drivers} водителей.")
        return

    df = pd.DataFrame(data)
    kmeans = KMeans(n_clusters=num_drivers, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    for i in range(num_drivers):
        driver_points = df[df['driver'] == i]
        result = f"🚚 **МАРШРУТ ВОДИТЕЛЯ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            # Для водителя убираем город для компактности
            short = row['address'].replace("Москва, ", "").replace("г. Москва, ", "")
            result += f"• {short}\n"
        await message.answer(result, parse_mode="Markdown")

    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
