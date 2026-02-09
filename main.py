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

# --- СУПЕР-ОЧИСТКА АДРЕСА (V7) ---
def clean_address(text):
    # 1. Извлечение блока
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)
    
    if not match: return None
    raw = match.group(1).replace('\n', ' ').strip()

    # 2. Предварительная чистка знаков
    raw = raw.replace('"', '').replace('«', '').replace('»', '')
    
    # 3. Список стоп-слов и паттернов
    # Регулярка для ФИО (2-3 слова с большой буквы)
    name_pattern = r'\b[А-Я][а-я]+\s+[А-Я][а-я]+(?:\s+[А-Я][а-я]+)?\b'
    
    # Разбиваем на части по запятой
    parts = raw.split(',')
    valid_parts = []
    seen_moscow = False

    for p in parts:
        p_clean = p.strip()
        p_low = p_clean.lower()

        # --- КРИТЕРИИ УДАЛЕНИЯ ЧАСТИ ---
        # 1. Если это ФИО (Абрамов Александр...)
        if re.fullmatch(name_pattern, p_clean): continue
        # 2. Если это ИНН/КПП/Счет (длинные цифры)
        if re.search(r'\d{8,25}', p_clean): continue
        # 3. Если это название компании (часто одно слово в кавычках, кавычки мы уже сняли)
        if p_clean in ["Скалка", "АЛЬФА-", "ПАО", "АО", "ООО", "ИП"]: continue
        # 4. Если это мусорные слова
        stop_words = ['р/с', 'к/с', 'бик', 'инн', 'кпп', 'банк', 'тел', 'г.']
        if any(sw in p_low for sw in stop_words): continue
        # 5. Если это Москва
        if "москва" in p_low:
            if not seen_moscow:
                valid_parts.append("Москва")
                seen_moscow = True
            continue
        
        # Если проверка пройдена — добавляем
        if len(p_clean) > 1:
            # Убираем одинокие "г" в конце части
            p_clean = re.sub(r'\s+[гГ]\.?$', '', p_clean)
            valid_parts.append(p_clean)

    # Собираем
    res = ", ".join(valid_parts)

    # 4. ФОРМАТИРОВАНИЕ УЛИЦ И ДОМОВ
    # Если "Москва" не в начале, переносим
    if "Москва" in res:
        res = res.replace("Москва, ", "").replace(", Москва", "").replace("Москва", "").strip(" ,")
        res = "Москва, " + res

    # Стандартизируем "ул."
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    
    # Исправляем дом и корпус (23 к1 -> 23к1)
    res = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    
    # Склеиваем номер и литеру (13 А -> 13А)
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)

    # Убираем "д." и "дом"
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE)

    # ГЛАВНОЕ: Ставим запятую перед номером дома, если её нет
    # (Ищем: Слово + пробел + число)
    res = re.sub(r'([а-яА-Я]{3,})\s+(\d+)', r'\1, \2', res)

    # 5. ФИНАЛЬНАЯ ЧИСТКА
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    return res.strip(' ,.')

# --- Геокодинг ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_bot_v7")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

# --- Обработчики AIOGRAM ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Версия V7 готова. Я научился удалять ФИО и названия компаний!")

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
                await message.answer(f"✅ **Адрес очищен:**\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❌ Не удалось распознать адрес.")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Пришли PDF!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Адресов: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("🔄 Строю маршруты...")
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: coords = get_coords(addr.split(',')[0] + "," + addr.split(',')[1])
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.1)

    if not data:
        await message.answer("Ошибка поиска на карте."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

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
