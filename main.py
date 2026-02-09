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

# --- Сервер для Render ---
async def handle_health(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()

# --- ОБНОВЛЕННАЯ ОЧИСТКА АДРЕСА (V18 - С УЛУЧШЕННЫМ ФОРМАТОМ ДОМОВ) ---
def clean_address(text):
    # 1. Извлечение блока
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер|Транспортная)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. Удаление индексов и кавычек
    raw = re.sub(r'["«»]', '', raw)
    raw = re.sub(r'\b\d{6}\b', '', raw)

    # 3. ТОТАЛЬНОЕ УДАЛЕНИЕ МУСОРА
    junk_patterns = [
        r'вн\.?тер\.?[^,]*', 
        r'муниципальный округ[^,]*', 
        r'\b(филиал|инн|кпп|бик|огрн|окпо)\b', 
        r'\b(ип|ооо|пао|ао|зао)\b.*?(?=москва|ул|пр|наб|$)', 
        r'\d{10,25}',
        r'\b(р/с|к/с|рс|кс)\b.*',
        r'банковские реквизиты.*',
        r'тел\..*'
    ]
    for p in junk_patterns:
        raw = re.sub(p, '', raw, flags=re.IGNORECASE)

    # 4. ПОИСК НАЧАЛА АДРЕСА
    anchor_pattern = re.compile(r'(Москва|ул\.|ул\s|пр-т|проспект|наб|пер\.|бульвар|шоссе|пл\.)', re.IGNORECASE)
    match_anchor = anchor_pattern.search(raw)
    if match_anchor:
        raw = raw[match_anchor.start():]

    # 5. РАЗБИВКА И ФИЛЬТРАЦИЯ
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False

    for p in parts:
        p_clean = p.strip()
        p_clean = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_clean, flags=re.IGNORECASE)
        
        if not p_clean: continue
        
        if "москва" in p_clean.lower():
            if not seen_moscow:
                clean_parts.append("Москва")
                seen_moscow = True
            continue
        
        # Фильтрация ФИО (только если это не название улицы типа Маршала Жукова)
        if not re.search(r'(ул\.|пр-т|пр\s|пер\.|шоссе)', p_clean, re.I):
            p_clean = re.sub(r'^([А-ЯЁ][а-яё]+\s*){2,3}', '', p_clean).strip()
        
        if len(p_clean) > 1:
            clean_parts.append(p_clean)
            
            # СТОП-КРАН
            is_house_number = re.match(r'^(\d+(?:[а-яА-Я]|\/|\-)?\d*|д\.|корп|стр|дом)\b', p_clean, re.IGNORECASE)
            is_street_with_digit = re.match(r'^\d+-?(ая|я|й|го)\b', p_clean, re.IGNORECASE)

            if is_house_number and not is_street_with_digit:
                 # Проверяем, нет ли слова "стр" или "корп" дальше в этой же части
                 if not re.search(r'(стр|корп|к\.)\s*\d+', p_clean, re.I):
                    break 

    # Сборка
    res = ", ".join(clean_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # 6. ФИНАЛЬНОЕ ФОРМАТИРОВАНИЕ (ВАШИ ПРАВКИ ТУТ)
    # Ставим точку после ул
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    
    # Склеиваем дома и корпуса: 10, к.1 -> 10к1; 12, корп.2 -> 12к2
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    
    # Удаляем слово "д." перед номером дома, если перед ним запятая
    res = re.sub(r',\s*(?:д\.|дом)\s*(\d+)', r', \1', res, flags=re.IGNORECASE)
    
    # Исправляем формат строений: 10стр12 -> 10 стр. 12
    res = re.sub(r'(\d+)\s*(?:стр\.?|с\.)\s*(\d+)', r'\1 стр. \2', res, flags=re.IGNORECASE)
    
    # Если между названием улицы и цифрой дома нет запятой — ставим её
    # Пример: Университетский пр-т 23 -> Университетский пр-т, 23
    res = re.sub(r'([а-яё\.]{2,})\s+(\d+[а-я]?)\b', r'\1, \2', res, flags=re.IGNORECASE)

    # Финальная чистка
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    res = res.replace("Москва, Москва", "Москва")
    
    return res.strip(' ,.')

# --- Все остальные функции остаются без изменений (V17) ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v18_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Бот V18. Исправлены форматы домов (10к1, стр. 12) и запятые.")

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
            await asyncio.sleep(0.5)
            if addr:
                if message.from_user.id not in user_data: user_data[message.from_user.id] = {'addresses': []}
                user_data[message.from_user.id]['addresses'].append(addr)
                await message.answer(f"✅ **Адрес:**\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❌ Ошибка в {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Сначала пришли PDF!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Адресов: {len(user_data[u_id]['addresses'])}. Водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    progress = await message.answer("🔄 Строю маршруты...")
    await bot.send_chat_action(message.chat.id, "find_location")
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1)
    if not data:
        await progress.edit_text("❌ Ошибка поиска."); return
    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_
    await progress.delete()
    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **МАРШРУТ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            res += f"📍 {row['address'].replace('Москва, ', '')}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
