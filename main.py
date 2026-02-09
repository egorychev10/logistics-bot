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

# --- УЛЬТИМАТИВНАЯ ОЧИСТКА (V11) ---
def clean_address(text):
    # 1. Извлечение блока
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. МГНОВЕННОЕ УДАЛЕНИЕ БАНКОВСКИХ СЧЕТОВ (любые 10-25 цифр)
    raw = re.sub(r'\d{10,25}', '', raw)
    
    # 3. УДАЛЕНИЕ ЮРЛИЦ И БАНКОВ
    # Добавляем конкретные названия банков, которые лезут в адрес
    raw = re.sub(r'\b(АЛЬФА-БАНК|АЛЬФА|БАНК|ФИЛИАЛ|ПАО|АО|ООО|ИП|ИНН|КПП|БИК|Р/С|К/С)\b.*', '', raw, flags=re.IGNORECASE)

    # 4. ЧИСТКА ОКРУГОВ (вн.тер и прочее)
    raw = re.sub(r'вн\.?тер\.?[^,]*', '', raw, flags=re.IGNORECASE)
    raw = re.sub(r'муниципальный округ[^,]*', '', raw, flags=re.IGNORECASE)

    # 5. СТАНДАРТИЗАЦИЯ МОСКВЫ
    # Убираем индексы (6 цифр)
    raw = re.sub(r'\b\d{6}\b', '', raw)
    
    # Ищем начало адреса (улица, проспект и т.д.)
    # Если есть "Москва", начинаем с неё, если нет — ищем улицу
    anchor = re.search(r'(Москва|ул\.|ул\s|пр-т|проспект|наб|пер\.|бульвар|шоссе|пл\.)', raw, re.IGNORECASE)
    if anchor:
        raw = raw[anchor.start():]

    # 6. ЧИСТКА ЧАСТЕЙ
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False

    for p in parts:
        p_clean = p.strip()
        # Удаляем "г." или "город" как отдельные слова
        p_clean = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_clean, flags=re.IGNORECASE)
        
        # Если в части остался мусор (счета или банки), игнорируем её
        if not p_clean or any(word in p_clean.upper() for word in ["БАНК", "СЧЕТ", "Р/С", "ИНН"]):
            continue

        if "москва" in p_clean.lower():
            if not seen_moscow:
                clean_parts.append("Москва")
                seen_moscow = True
            continue
        
        clean_parts.append(p_clean)

    res = ", ".join(clean_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # 7. ФИНАЛЬНОЕ ПРИЧЕСЫВАНИЕ
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    # Склейка корпусов 23 к1 -> 23к1
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    # Ставим запятую перед домом, если её нет
    res = re.sub(r'([а-яА-ЯёЁ]{3,})\s+(\d+)', r'\1, \2', res)
    
    # Удаляем любые висящие в конце цифры или мусорные буквы
    res = re.sub(r'[, ]+\d{10,}$', '', res)
    res = re.sub(r'[, ]+[а-яА-Я]$', '', res)

    return res.strip(' ,.')

# --- ГЕОКОДИНГ С ЗАЩИТОЙ ОТ LIMITS ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v11_safety")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except:
        return None

# --- AIOGRAM HANDLERS ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Версия V11. Банковские счета и мусор теперь вырезаются полностью.")

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
        await message.answer("Сначала пришлите PDF!"); return
    
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено адресов: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    progress = await message.answer("⏳ **Обработка данных и поиск координат...**")
    
    data = []
    for addr in raw_addresses:
        # Chat action для визуализации
        await bot.send_chat_action(message.chat.id, "find_location")
        coords = get_coords(addr)
        if not coords:
            coords = get_coords(", ".join(addr.split(',')[:2]))
        
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        
        # Задержка 1.2 сек для обхода лимитов Nominatim и Telegram
        await asyncio.sleep(1.2)

    if not data:
        await progress.edit_text("❌ Ошибка: не удалось найти адреса на карте."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    await progress.delete()

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
