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

# --- ЭТАЛОННАЯ ОЧИСТКА V14 (ОСНОВА V9) ---
def clean_address(text):
    # 1. ИЗОЛЯЦИЯ БЛОКА ГРУЗОПОЛУЧАТЕЛЯ
    # Ищем только то, что относится к получателю, отсекая верхушку (отправителя)
    match = re.search(r"Грузополучатель(.*?)(?:Поставщик|Основание|Транспортная|Пункт)", text, re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. УДАЛЕНИЕ МУСОРА (Реквизиты и Индексы)
    # Удаляем счета (10-25 цифр), индексы (6 цифр) и ИНН/КПП (9-12 цифр)
    raw = re.sub(r'\b\d{10,25}\b', '', raw)
    raw = re.sub(r'\b\d{6}\b', '', raw)
    
    # Список слов-исключений (вырезаем целиком)
    junk = [
        r'вн\.?тер\.?\s*г\.?[^,]*', r'муниципальный округ[^,]*', 
        r'Общество[^,]*', r'ООО', r'ИП', r'АО', r'ИНН', r'КПП', r'ОКПО',
        r'р/с', r'к/с', r'бик', r'банк', r'филиал', r'тел\.', r'факс'
    ]
    for pattern in junk:
        raw = re.sub(pattern, '', raw, flags=re.IGNORECASE)

    # 3. ПОИСК ГЕО-ЯКОРЯ (Где начинается адрес)
    # Ищем "Москва" или признаки улицы
    anchor = re.search(r'(Москва|ул\.|пр-т|проспект|наб|пер\.|бульвар|шоссе|площадь)', raw, re.IGNORECASE)
    if anchor:
        raw = raw[anchor.start():]

    # 4. ФИЛЬТРАЦИЯ ЧАСТЕЙ
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False

    for p in parts:
        p_strip = p.strip()
        # Удаляем "г." или "город" только как ОТДЕЛЬНЫЕ слова (Нижегородская не пострадает)
        p_strip = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_strip, flags=re.IGNORECASE)
        
        # Убираем ФИО, если они затесались (2-3 слова с большой буквы в начале)
        p_strip = re.sub(r'^([А-ЯЁ][а-яё]+\s*){2,3}', '', p_strip).strip()

        if "москва" in p_strip.lower():
            if not seen_moscow:
                clean_parts.append("Москва")
                seen_moscow = True
            continue
            
        # Если в части есть хоть одна цифра (дом) или признак улицы — берем
        if re.search(r'\d', p_strip) or re.search(r'(ул\.|пр-т|наб|пер|пр\.)', p_strip, re.IGNORECASE):
            # Убираем кавычки
            p_strip = p_strip.replace('"', '').replace('«', '').replace('»', '')
            if len(p_strip) > 1:
                clean_parts.append(p_strip)

    if not clean_parts: return None
    
    # 5. СБОРКА И ФИНАЛЬНОЕ ФОРМАТИРОВАНИЕ
    res = ", ".join(clean_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # Правка: ул Бажова 4 -> ул. Бажова, 4
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    res = re.sub(r'([а-яА-ЯёЁ]{3,})\s+(\d+)', r'\1, \2', res)
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE)

    return res.strip(' ,.')

# --- ГЕОКОДИНГ ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v14_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

# --- ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Версия V14 (База V9 + фикс реквизитов). Жду файлы!")

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
                await message.answer(f"❌ Не удалось найти адрес.")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Пришли файлы!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    status = await message.answer("⏳ **Обработка маршрутов...**")
    data = []
    for addr in raw_addresses:
        await bot.send_chat_action(message.chat.id, "find_location")
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.2)

    if not data:
        await status.edit_text("❌ Ошибка поиска."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_
    await status.delete()

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **ВОДИТЕЛЬ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            res += f"📍 {row['address'].replace('Москва, ', '')}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
