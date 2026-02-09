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

# --- Сервер для поддержания жизни на Render ---
async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- ЭТАЛОННАЯ ОЧИСТКА АДРЕСА (V8) ---
def clean_address(text):
    # 1. Извлечение сырого блока
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. Первичная чистка: кавычки и индексы (6 цифр)
    raw = re.sub(r'["«»]', '', raw)
    raw = re.sub(r'\b\d{6}\b', '', raw) # Удаляем индексы

    # 3. ПОИСК "ЯКОРЯ" (Где начинается сам адрес)
    # Ищем: Москва, ул, пр-т, пр., набережная, переулок, шоссе
    anchor_pattern = re.compile(r'(Москва|ул\.|ул\s|пр-т|проспект|наб|пер\.|переулок|бульвар|шоссе|площадь)', re.IGNORECASE)
    match_anchor = anchor_pattern.search(raw)
    
    if match_anchor:
        # Отрезаем всё, что ДО начала адреса (ИП, ФИО, названия)
        raw = raw[match_anchor.start():]
    else:
        # Если якорь не найден, пробуем убрать ФИО в начале (2-3 слова с заглавной)
        raw = re.sub(r'^([А-ЯЁ][а-яё]+\s*){2,3}', '', raw).strip()

    # 4. Удаление оставшегося мусора (ИП, ООО, Банки)
    junk_patterns = [
        r'\b(ИП|ООО|ПАО|АО|ЗАО)\b.*?(?=Москва|ул|пр|наб|$)', # Юрлица
        r'\b\d{10,25}\b', # ИНН/Счета
        r'р/с|к/с|бик|инн|кпп|банк|тел|г\.|город', # Служебные слова
    ]
    for p in junk_patterns:
        raw = re.sub(p, '', raw, flags=re.IGNORECASE)

    # 5. Сборка и форматирование
    # Разделяем на части, убираем пустые
    parts = [p.strip() for p in raw.split(',') if len(p.strip()) > 1]
    
    # Убираем дубли Москвы и формируем костяк
    final_parts = []
    seen_moscow = False
    for p in parts:
        if "москва" in p.lower():
            seen_moscow = True
            continue
        final_parts.append(p)
    
    res = ", ".join(final_parts)
    
    # ПРИНУДИТЕЛЬНО ставим Москву в начало
    res = "Москва, " + res.strip(" ,")

    # 6. КРАСИВОЕ ФОРМАТИРОВАНИЕ (как в ТЗ)
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE) # ул -> ул.
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE) # 23, к1 -> 23к1
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res) # 13 А -> 13А
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE) # Удаляем "д."
    
    # Ставим запятую перед номером дома, если её забыли
    res = re.sub(r'([а-яА-Я]{3,})\s+(\d+)', r'\1, \2', res)

    # Чистим двойные пробелы и запятые
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    
    return res.strip(' ,.')

# --- Логика Группировки и Маршрутов ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v8_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Версия V8 загружена. Теперь адреса будут идеально чистыми!", reply_markup=markup)

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
                await message.answer(f"❌ Ошибка в файле {message.document.file_name}")
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
    
    await message.answer("🔄 Геокодирую и считаю маршруты...")
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: # Поиск по упрощенному адресу
            coords = get_coords(", ".join(addr.split(',')[:2]))
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
