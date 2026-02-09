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

# --- ИСПРАВЛЕННАЯ ФУНКЦИЯ ОЧИСТКИ АДРЕСА ---
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

    # 5. РАЗБИВКА И ФИЛЬТРАЦИЯ ЧАСТЕЙ
    parts = raw.split(',')
    clean_parts = []
    seen_moscow = False
    street_detected = False
    last_was_street_name = False  # Для отслеживания, была ли предыдущая часть названием улицы

    for i, p in enumerate(parts):
        p_clean = p.strip()
        # Удаляем "г."
        p_clean = re.sub(r'\b(г\.|г|город)\b\.?\s*', '', p_clean, flags=re.IGNORECASE)
        
        if not p_clean: continue
        
        # Если встречаем "москва" - добавляем один раз
        if "москва" in p_clean.lower():
            if not seen_moscow:
                clean_parts.append("Москва")
                seen_moscow = True
            continue
        
        # Убираем ФИО
        p_clean = re.sub(r'^([А-ЯЁ][а-яё]+\s*){2,3}', '', p_clean).strip()
        
        if not p_clean: continue
        
        # Определяем, является ли часть номером дома/корпусом
        is_house_number = re.match(r'^\d+[а-яА-Я]?$', p_clean) or re.match(r'^\d+к\d+$', p_clean) or re.match(r'^\d+\s*стр\.', p_clean, re.IGNORECASE)
        is_building = re.match(r'^(к|корп|стр|строение|с)\.?\s*\d*', p_clean, re.IGNORECASE)
        
        # Если это номер дома или корпус, добавляем без изменений
        if is_house_number or is_building:
            clean_parts.append(p_clean)
            last_was_street_name = False
            continue
        
        # Определяем, является ли часть улицей (содержит ключевые слова улиц)
        is_street_type = re.search(r'\b(ул|улица|пр-т|проспект|пер|переулок|наб|набережная|б-р|бульвар|ш|шоссе)\b', p_clean, re.IGNORECASE)
        
        # Если это тип улицы
        if is_street_type:
            street_type = is_street_type.group(1).lower()
            if street_type in ['ул', 'улица']:
                p_clean = re.sub(r'\b(ул|улица)\b', 'ул.', p_clean, flags=re.IGNORECASE)
            clean_parts.append(p_clean)
            street_detected = True
            last_was_street_name = False
            continue
        
        # Если это явно название улицы без указания типа - добавляем "ул."
        if not street_detected and not re.match(r'^\d', p_clean) and len(p_clean.split()) >= 1:
            # Проверяем, не содержит ли уже тип улицы
            if not re.search(r'\b(ул\.|проспект|пер\.|бульвар|шоссе|набережная|пл\.)\b', p_clean, re.IGNORECASE):
                # Добавляем "ул." только если это похоже на название улицы
                if re.search(r'[а-яё]{3,}', p_clean.lower()):
                    # Проверяем, что это не номер дома и не корпус/строение
                    if not re.match(r'^\d+[а-я]?$', p_clean) and not re.match(r'^(к|корп|стр|строение|с)', p_clean, re.IGNORECASE):
                        p_clean = f"ул. {p_clean}"
                        street_detected = True
                        last_was_street_name = True
        elif last_was_street_name and re.match(r'^[А-Яа-яёЁ]+', p_clean):
            # Если предыдущая часть была названием улицы, а текущая тоже начинается с букв,
            # то это продолжение названия улицы
            if clean_parts and clean_parts[-1].startswith('ул.'):
                clean_parts[-1] = clean_parts[-1] + ' ' + p_clean
                continue
        
        if len(p_clean) > 0:
            clean_parts.append(p_clean)
            last_was_street_name = False

    # Сборка
    res = ", ".join(clean_parts)
    if not res.startswith("Москва"):
        res = "Москва, " + res.lstrip(" ,")

    # 6. ФИНАЛЬНОЕ ФОРМАТИРОВАНИЕ
    # Удаление двойных "ул."
    res = re.sub(r'ул\.\s+ул\.', 'ул.', res, flags=re.IGNORECASE)
    
    # Удаление двойных точек и лишних пробелов в "ул.."
    res = re.sub(r'ул\.\.', 'ул.', res, flags=re.IGNORECASE)
    
    # Унификация обозначений улиц
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'\bпер\b(?!\.)', 'пер.', res, flags=re.IGNORECASE)
    
    # Преобразование сокращений
    res = re.sub(r'\bпр-т\b', 'проспект', res, flags=re.IGNORECASE)
    res = re.sub(r'\bнаб\.\b', 'набережная', res, flags=re.IGNORECASE)
    
    # Удаление "д." и "дом" - многоступенчатый подход
    # 1. Удаляем в начале строки
    res = re.sub(r'^д\.|^дом\s+', '', res, flags=re.IGNORECASE)
    # 2. Удаляем после запятой
    res = re.sub(r',\s*д\.\s*', ', ', res, flags=re.IGNORECASE)
    res = re.sub(r',\s*дом\s*', ', ', res, flags=re.IGNORECASE)
    # 3. Удаляем в середине строки
    res = re.sub(r'\s+д\.\s+', ' ', res, flags=re.IGNORECASE)
    res = re.sub(r'\s+дом\s+', ' ', res, flags=re.IGNORECASE)
    
    # Удаление "д." перед номерами
    res = re.sub(r'д\.(\d+)', r'\1', res, flags=re.IGNORECASE)
    res = re.sub(r'дом(\d+)', r'\1', res, flags=re.IGNORECASE)
    
    # Объединение номера дома и корпуса/строения
    res = re.sub(r'(\d+[А-Яа-я]?)\s*[,]?\s*(?:корп\.?|к\.?|к)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+[А-Яа-я]?)\s*[,]?\s*(?:стр\.?|строение|с\.?)\s*(\d+)', r'\1 стр. \2', res, flags=re.IGNORECASE)
    
    # Объединение буквы с номером дома
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)
    
    # Разделение названия улицы и номера дома, если они слиты
    # Универсальный паттерн для всех типов улиц
    res = re.sub(r'([а-яА-ЯёЁ]{2,}(?:\s+[а-яА-ЯёЁ]+){0,3})\s+(\d+[а-яА-Я]?\d*(?:к\d+)?)', r'\1, \2', res)
    
    # Удаление лишних запятых и пробелов
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    res = re.sub(r',\s*,', ', ', res)
    
    # Удаление запятой перед корпусом/строением
    res = re.sub(r',\s*(к\d+|стр\.\s*\d+)', r' \1', res)
    
    # Удаление "д." в конце адреса
    res = re.sub(r',\s*д\.\s*$', '', res, flags=re.IGNORECASE)
    
    # Исправление для адресов типа "ул. Проспект Мира" - убираем лишнее "ул."
    res = re.sub(r'ул\.\s+(проспект|пер\.|бульвар|шоссе|набережная|пл\.)', r'\1', res, flags=re.IGNORECASE)
    
    # Исправление для адресов типа "Университетский проспект д, 23к1" - убираем "д,"
    res = re.sub(r'\b(проспект|ул\.|пер\.|бульвар|шоссе|набережная)\s+д\s*,', r'\1,', res, flags=re.IGNORECASE)
    
    # Исправление для адресов типа "1-ая Останкинская ул.." - убираем лишние точки
    res = re.sub(r'ул\.\.', 'ул.', res)
    
    # Удаление лишних запятых
    res = re.sub(r',\s*,', ',', res)
    
    # Финальная чистка - убираем двойные пробелы
    res = re.sub(r'\s+', ' ', res).strip()
    
    # Удаление запятых в начале
    res = re.sub(r'^,\s*', '', res)
    
    # Проверка, что у нас есть улица в адресе
    # Если адрес содержит только "Москва, 39к1" - добавляем ул. к следующей части
    if re.match(r'^Москва,\s*\d', res):
        # Находим первую часть после Москвы
        match = re.match(r'^Москва,\s*([^,]+)', res)
        if match:
            after_moscow = match.group(1)
            # Если это не похоже на улицу, а похоже на номер дома
            if re.match(r'^\d', after_moscow):
                # Ищем в исходном тексте название улицы
                street_match = re.search(r'([А-Яа-яёЁ]+\s+[А-Яа-яёЁ]+)(?=\s*\d)', raw)
                if street_match:
                    street_name = street_match.group(1)
                    res = f"Москва, ул. {street_name}, {after_moscow}"
    
    return res.strip(' ,.')

# --- Логика Геокодинга и Маршрутов ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v17_stable")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Бот V17 готов. Загрузка файлов оптимизирована (без ошибок флуда).")

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
                await message.answer(f"❌ Ошибка распознавания в {message.document.file_name}")
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
    
    progress = await message.answer("🔄 Строю маршруты...")
    
    await bot.send_chat_action(message.chat.id, "find_location")

    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        
        await asyncio.sleep(1.1)

    if not data:
        await progress.edit_text("❌ Ошибка поиска на карте."); return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    await progress.delete()

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
