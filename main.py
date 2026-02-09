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

TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- ЭТАЛОННАЯ ОЧИСТКА V12 ---
def clean_address(text):
    # 1. Извлекаем сырой блок из ТОРГ-12
    pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер|Склад)", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. УДАЛЯЕМ ТОЛЬКО БАНКОВСКИЕ СЧЕТА И ИНДЕКСЫ
    # Удаляем любые числа от 10 до 25 знаков (счета) и 6 знаков (индексы)
    raw = re.sub(r'\b\d{10,25}\b', '', raw)
    raw = re.sub(r'\b\d{6}\b', '', raw)

    # 3. УДАЛЯЕМ МУСОРНЫЕ СЛОВА (не затрагивая названия улиц)
    junk = [r'\bИП\b', r'\bООО\b', r'\bПАО\b', r'\bАО\b', r'банк', r'филиал', r'расчетный счет', r'инн', r'кпп', r'бик']
    for j in junk:
        raw = re.sub(j, '', raw, flags=re.IGNORECASE)

    # 4. ВЫДЕЛЯЕМ ГЕОГРАФИЧЕСКУЮ ЧАСТЬ
    # Ищем упоминание Москвы или ключевых слов адреса
    geo_pattern = re.compile(r'(Москва|ул\.|пр-т|пр\.|проспект|наб|пер\.|бульвар|шоссе|пл\.|д\.|корп\.|стр\.|к\.)', re.IGNORECASE)
    parts = raw.split(',')
    valid_parts = []
    
    start_collecting = False
    for p in parts:
        p_clean = p.strip()
        # Если в части есть гео-маркер или это похоже на номер дома (цифра + буква)
        if geo_pattern.search(p_clean) or re.search(r'\b\d+[а-яА-Я]?\b', p_clean):
            start_collecting = True
        
        if start_collecting:
            # Чистим часть от остатков мусора
            p_clean = re.sub(r'["«»]', '', p_clean)
            p_clean = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_clean, flags=re.IGNORECASE)
            
            # Если часть содержит только "Москва", не дублируем её
            if p_clean.lower() == "москва":
                if "Москва" not in valid_parts:
                    valid_parts.append("Москва")
                continue
            
            if len(p_clean) > 0:
                valid_parts.append(p_clean)

    # Собираем результат
    if not valid_parts: return None
    
    res = ", ".join(valid_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # 5. ФИНАЛЬНАЯ ПРАВКА ФОРМАТА
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    # Форматируем дома: 23 к 1 -> 23к1
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    # Ставим запятую перед домом, если пропущена
    res = re.sub(r'([а-яА-ЯёЁ]{4,})\s+(\d+)', r'\1, \2', res)
    # Склеиваем литеры: 13 А -> 13А
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)
    
    # Удаляем вн.тер и муниципальные округа
    res = re.sub(r'вн\.?тер\.?[^,]*', '', res, flags=re.IGNORECASE)
    res = re.sub(r'муниципальный округ[^,]*', '', res, flags=re.IGNORECASE)

    return res.strip(' ,.')

# --- ГЕОКОДИНГ ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v12_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

# --- ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Версия V12 (Исправлено исчезновение адресов). Жду PDF!", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'): return
    await bot.send_chat_action(message.chat.id, "typing")
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
                await message.answer(f"❌ Не смог извлечь адрес из {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Пришли сначала накладные!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    status = await message.answer("⏳ **Ищу адреса на карте...**")
    data = []
    for addr in raw_addresses:
        await bot.send_chat_action(message.chat.id, "find_location")
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.2)

    if not data:
        await status.edit_text("❌ Ошибка поиска координат."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_
    await status.delete()

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **МАРШРУТ ВОДИТЕЛЯ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            final_view = row['address'].replace("Москва, ", "")
            res += f"📍 {final_view}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
