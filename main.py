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

# Настройки
TOKEN = os.getenv("BOT_TOKEN")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Производственная, 1")

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

# --- Новая мощная очистка адреса ---
def clean_address(text):
    # 1. Ищем блок адреса (между ОКПД и Грузополучателем, как ты просил)
    # Используем широкий захват, так как в PDF текст может "плавать"
    pattern = re.compile(r"Вид деятельности по ОКПД(.*?)Грузополучатель", re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    
    if not match:
        # Запасной вариант, если блок смещен
        pattern = re.compile(r"Грузополучатель(.*?)(?:Поставщик|Основание|Номер)", re.DOTALL | re.IGNORECASE)
        match = pattern.search(text)

    if not match:
        return None

    res = match.group(1).replace('\n', ' ').strip()

    # 2. Удаляем ИНН, КПП, расчетные счета (длинные цифры) и индексы
    res = re.sub(r'\d{10,25}', '', res) # Банковские реквизиты
    res = re.sub(r'\b\d{6}\b', '', res) # Индексы

    # 3. Находим начало адреса (отсекаем ИП, ООО и прочее в начале)
    # Ищем первое упоминание Москвы или города
    start_match = re.search(r'(?:г\.|г\s|город|москва)', res, re.IGNORECASE)
    if start_match:
        res = res[start_match.start():]

    # 4. Форматируем город (убираем г., г, город)
    res = re.sub(r'^(?:г\.|г\s|город|Город)\s*', '', res, flags=re.IGNORECASE)
    res = re.sub(r'^Москва\s*', '', res, flags=re.IGNORECASE)
    
    # 5. Чистим мусорные слова (округа, районы, административные данные)
    junk = [
        r'вн\.тер\.г\.', r'муниципальный округ', r'административный округ', 
        r'помещение', r'офис', r'этаж', r'бизнес-центр', r'БЦ', r'ИНН', r'КПП'
    ]
    for pattern in junk:
        res = re.sub(pattern, '', res, flags=re.IGNORECASE)

    # 6. Стандартизируем "ул."
    # Если есть "ул" без точки, ставим точку
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    
    # 7. Форматируем дома и корпуса
    # Убираем "д." или "дом" перед номером дома
    res = re.sub(r',\s*(?:д\.|дом)\s*', ', ', res, flags=re.IGNORECASE)
    # Склеиваем корпус: "102, корп. 1" -> "102к1" или "102 корп 1" -> "102к1"
    res = re.sub(r'[, ]*(?:корп\.?|к\.)\s*(\d+)', r'к\1', res, flags=re.IGNORECASE)
    # Убираем "стр." (оставляем как есть, но чистим пробелы)
    res = re.sub(r'\s*стр\.\s*', ', стр. ', res, flags=re.IGNORECASE)

    # 8. Финальная сборка
    # Убираем двойные запятые и лишние пробелы
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    res = res.strip(' ,.()')
    
    # Всегда добавляем Москву в начало
    return f"Москва, {res}"

def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v4_geocoder")
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
    await message.answer("Пришли мне PDF. Я почищу адреса по новому алгоритму.", reply_markup=markup)

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
                await message.answer(f"✅ **Адрес принят:**\n`{addr}`", parse_mode="Markdown")
            else:
                await message.answer(f"❌ Не нашел адрес в {message.document.file_name}")
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
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    await message.answer("🔄 Строю маршруты... Это может занять до минуты.")
    
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        else:
            # Если точный адрес не найден, пробуем без номера дома/корпуса для геокодинга
            simple_addr = addr.split(',')[0] + "," + addr.split(',')[1]
            coords = get_coords(simple_addr)
            if coords:
                data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1)

    if len(data) < 1:
        await message.answer("Не удалось найти координаты адресов.")
        return

    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        result = f"🚛 **МАРШРУТ ВОДИТЕЛЯ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            # Убираем "Москва, " для списка водителю
            final_view = row['address'].replace("Москва, ", "")
            result += f"📍 {final_view}\n"
        await message.answer(result, parse_mode="Markdown")

    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
