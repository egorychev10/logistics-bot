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
bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# --- Вспомогательный сервер для Render ---
async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- УЛЬТРА-ОЧИСТКА АДРЕСА (V6) ---
def clean_address(text):
    # 1. Извлечение блока адреса
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. Удаление индексов (6 цифр) и банковских счетов
    raw = re.sub(r'\b\d{6}\b', '', raw) 
    raw = re.sub(r'\d{10,25}', '', raw)

    # 3. Список мусора, который удаляем полностью (в любом регистре)
    junk_to_remove = [
        r'\bАО\b', r'\bПАО\b', r'\bООО\b', r'\bИП\b', r'\bр/с\b', r'\bк/с\b', r'\bБИК\b',
        r'расчетный счет', r'инн', r'кпп', r'банк', r'филиал', r'общество',
        r'вн\.тер\.г\.', r'муниципальный округ', r'административный округ',
        r'ростокино', r'головинский', r'академический' # И другие районы, если лезут
    ]
    for j in junk_to_remove:
        raw = re.sub(j, '', raw, flags=re.IGNORECASE)

    # 4. Разбиваем по запятым, чистим части и убираем дубли города
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False

    for p in parts:
        p_clean = p.strip()
        # Убираем "г.", "город"
        p_clean = re.sub(r'^(г\.|г\s|город|Город)\s*', '', p_clean, flags=re.IGNORECASE)
        
        # Обработка Москвы
        if "москва" in p_clean.lower():
            if seen_moscow: continue # Пропускаем вторую Москву
            p_clean = "Москва"
            seen_moscow = True
        
        if len(p_clean) > 1:
            clean_parts.append(p_clean)

    # Собираем строку
    res = ", ".join(clean_parts)

    # 5. КОРРЕКЦИЯ ФОРМАТА (ДОМ, КОРПУС, ЛИТЕРА)
    # Ставим точку после ул, если её нет
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    
    # Склеиваем "13 А" в "13А"
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)

    # Убираем "д." и "дом"
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE)

    # Форматируем корпус: "23, к1" или "23 к.1" -> "23к1"
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    
    # Если между названием улицы и номером дома нет запятой — ставим её
    # (Ищем: Слово + пробел + цифра)
    res = re.sub(r'([а-яА-Я]{3,})\s+(\d+)', r'\1, \2', res)

    # 6. ФИНАЛЬНЫЕ ШТРИХИ
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")
    
    # Убираем лишние запятые и пробелы
    res = re.sub(r'[,]{2,}', ',', res)
    res = re.sub(r'\s+', ' ', res)
    return res.strip(' ,.')

# --- Логика Геокодинга ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistics_bot_v6")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

# --- Обработчики AIOGRAM ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Пришли PDF. Я научился удалять АО, ПАО, индексы и дубли города!", reply_markup=markup)

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
                await message.answer(f"✅ **Чистый адрес:**\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❓ Ошибка распознавания в {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Нет данных!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Адресов: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("🔄 Геокодирую...")
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: coords = get_coords(addr.split(',')[0] + "," + addr.split(',')[1])
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1)

    if not data:
        await message.answer("Не удалось найти адреса на карте."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **ВОДИТЕЛЬ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            final_view = row['address'].replace("Москва, ", "")
            res += f"📍 {final_view}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
