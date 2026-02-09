import os
import re
import time
import asyncio
import pdfplumber
import pandas as pd
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from geopy.geocoders import Nominatim
from sklearn.cluster import KMeans
import requests

# Настройки из переменных окружения
TOKEN = os.getenv("BOT_TOKEN")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Производственная, 1")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Временное хранилище данных
user_data = {}

def clean_address(text):
    """Очистка адреса ТОРГ-12"""
    # Ищем блок между ОКПД и Грузополучателем
    match = re.search(r"Вид деятельности по ОКПД(.*?)(?:Грузополучатель|Телефон|ИНН)", text, re.S)
    if not match:
        return None
    
    addr = match.group(1).replace('\n', ' ').strip()
    # Убираем индексы (6 цифр), лишние знаки и слова
    addr = re.sub(r'\b\d{6}\b', '', addr)
    addr = re.sub(r'\(.*?\)', '', addr)
    addr = re.sub(r'[^а-яА-Я0-9\s,.-]', '', addr)
    # Финальная чистка
    addr = addr.split(', ,')[-1].strip()
    return "Москва, " + addr if "Москва" not in addr else addr

def get_coords(address):
    """Геокодирование через Nominatim"""
    try:
        geolocator = Nominatim(user_agent="my_logistics_bot_v1")
        location = geolocator.geocode(address)
        time.sleep(1) # Лимит Nominatim - 1 запрос в сек
        if location:
            return (location.latitude, location.longitude)
    except:
        return None
    return None

def get_route_dist(p1, p2):
    """Дистанция через бесплатный OSRM"""
    url = f"http://router.project-osrm.org/route/v1/driving/{p1[1]},{p1[0]};{p2[1]},{p2[0]}?overview=false"
    try:
        r = requests.get(url).json()
        return r['routes'][0]['distance']
    except:
        return 0

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'files': [], 'addresses': []}
    kb = [[KeyboardButton(text="🚚 Начать обработку накладных")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Привет! Перешли мне PDF-накладные (ТОРГ-12), а когда закончишь — нажми кнопку.", reply_markup=markup)

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.endswith('.pdf'):
        return
    
    file = await bot.get_file(message.document.file_id)
    file_path = file.file_path
    await bot.download_file(file_path, "temp.pdf")
    
    with pdfplumber.open("temp.pdf") as pdf:
        text = "".join([page.extract_text() for page in pdf.pages])
        addr = clean_address(text)
        if addr:
            user_data[message.from_user.id]['addresses'].append(addr)
            await message.answer(f"✅ Адрес найден: {addr}")
        else:
            await message.answer("❌ Не удалось найти адрес в этом файле.")
    os.remove("temp.pdf")

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    count = len(user_data[message.from_user.id]['addresses'])
    if count == 0:
        await message.answer("Сначала пришли мне PDF файлы!")
        return
    
    kb = [[KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
          [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Загружено адресов: {count}. На сколько водителей распределить?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    addresses = list(set(user_data[user_id]['addresses'])) # Убираем дубли
    
    await message.answer("⏳ Рассчитываю маршруты... Это может занять пару минут.")
    
    # Геокодирование
    data = []
    prod_coords = get_coords(PRODUCTION_ADDRESS)
    
    for addr in addresses:
        coords = get_coords(addr)
        if coords:
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
    
    if not data:
        await message.answer("Не удалось определить координаты ни одного адреса.")
        return

    df = pd.DataFrame(data)
    
    # Кластеризация (распределение по водителям)
    n_clusters = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_

    # Формирование ответа
    for i in range(n_clusters):
        driver_points = df[df['driver'] == i].copy()
        # Простая сортировка по удаленности от производства (ближайший-дальше)
        driver_points['dist_to_prod'] = driver_points.apply(lambda x: get_route_dist(prod_coords, (x['lat'], x['lon'])), axis=1)
        driver_points = driver_points.sort_values(by='dist_to_prod')
        
        result_text = f"📋 **ВОДИТЕЛЬ №{i+1}**\n"
        for idx, row in driver_points.iterrows():
            # Убираем "Москва" из вывода
            clean_print = row['address'].replace("Москва, ", "").replace("город Москва, ", "")
            result_text += f"📍 {clean_print}\n"
        
        await message.answer(result_text)

    await message.answer("Готово! Все маршруты построены.", reply_markup=types.ReplyKeyboardRemove())
    user_data[user_id] = {'files': [], 'addresses': []}

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
