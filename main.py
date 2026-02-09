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

# --- ФИНАЛЬНАЯ ЛОГИКА ОЧИСТКИ (V10) ---
def clean_address(text):
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 1. Сразу удаляем индексы, кавычки и ФИЛИАЛЫ
    raw = re.sub(r'\b\d{6}\b', '', raw)
    raw = re.sub(r'["«»]', '', raw)
    raw = re.sub(r'\b(филиал|р/с|к/с|бик|инн|кпп|ао|пао|ооо|ип)\b', '', raw, flags=re.IGNORECASE)

    # 2. Удаляем вн.тер. и любые приклеенные к ним названия округов
    # Находит "вн.тер.Ростокино", "вн.тер. муниципальный округ" и т.д.
    raw = re.sub(r'вн\.?тер\.?[^,]*', '', raw, flags=re.IGNORECASE)
    raw = re.sub(r'\b(муниципальный|административный)\s+округ\b[^,]*', '', raw, flags=re.IGNORECASE)

    # 3. ПОИСК ЯКОРЯ (Начало адреса)
    anchor = re.search(r'(Москва|ул\.|ул\s|пр-т|проспект|наб|пер\.|бульвар|шоссе|пл\.)', raw, re.IGNORECASE)
    if anchor:
        raw = raw[anchor.start():]

    # 4. РАЗБИВКА ПО ЗАПЯТЫМ И ОЧИСТКА ЧАСТЕЙ
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False

    for p in parts:
        p_clean = p.strip()
        # Удаляем "г." или "город" только как отдельные слова (чтобы не убить Нижегородскую)
        p_clean = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_clean, flags=re.IGNORECASE)
        
        # Убираем ФИО и названия в начале части
        p_clean = re.sub(r'^([А-ЯЁ][а-яё]+\s*){2,3}', '', p_clean).strip()
        
        if "москва" in p_clean.lower():
            if not seen_moscow:
                clean_parts.append("Москва")
                seen_moscow = True
            continue
            
        if len(p_clean) > 1:
            clean_parts.append(p_clean)

    res = ", ".join(clean_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # 5. КРАСИВОЕ ФОРМАТИРОВАНИЕ
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE)
    
    # Запятая перед домом (Улица 10 -> Улица, 10)
    res = re.sub(r'([а-яА-ЯёЁ]{3,})\s+(\d+)', r'\1, \2', res)

    # 6. ФИНАЛЬНЫЙ УДАР ПО ХВОСТАМ (р/с, АО в конце строки)
    res = re.sub(r'[, ]+\b(р/с|ао|пао|филиал|г)\.?\s*$', '', res, flags=re.IGNORECASE)
    
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    return res.strip(' ,.')

# --- ГЕОКОДИНГ ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v10_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

# --- ОБРАБОТЧИКИ ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Версия V10. Исправлены 'вн.тер', 'р/с' и добавлена индикация загрузки.", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'): return
    
    # Визуальная индикация обработки
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
                await message.answer(f"❌ Не найден адрес в {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Сначала загрузите PDF!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    status_msg = await message.answer("⏳ **Обработка...** Пожалуйста, подождите.")
    await bot.send_chat_action(message.chat.id, "find_location")

    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1)

    if not data:
        await status_msg.edit_text("❌ Ошибка: координаты не найдены."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    await status_msg.delete() # Удаляем "обработка"

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **МАРШРУТ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            final_view = row['address'].replace("Москва, ", "")
            res += f"📍 {final_view}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
