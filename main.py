import os
import re
import asyncio
import json
import pdfplumber
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiohttp
from aiohttp import web
import asyncio
from sklearn.cluster import KMeans
from collections import defaultdict

# --- Конфигурация ---
TOKEN = os.getenv("BOT_TOKEN")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# --- Состояния для FSM ---
class RouteStates(StatesGroup):
    waiting_for_departure_time = State()
    waiting_for_return_settings = State()
    editing_routes = State()
    moving_address = State()

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
    last_was_street_name = False

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
    res = re.sub(r'ул\.\s+ул\.', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'ул\.\.', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'\bул\b(?!\.)', 'ул.', res, flags=re.IGNORECASE)
    res = re.sub(r'\bпер\b(?!\.)', 'пер.', res, flags=re.IGNORECASE)
    res = re.sub(r'\bпр-т\b', 'проспект', res, flags=re.IGNORECASE)
    res = re.sub(r'\bнаб\.\b', 'набережная', res, flags=re.IGNORECASE)
    
    # Удаление "д." и "дом"
    res = re.sub(r'^д\.|^дом\s+', '', res, flags=re.IGNORECASE)
    res = re.sub(r',\s*д\.\s*', ', ', res, flags=re.IGNORECASE)
    res = re.sub(r',\s*дом\s*', ', ', res, flags=re.IGNORECASE)
    res = re.sub(r'\s+д\.\s+', ' ', res, flags=re.IGNORECASE)
    res = re.sub(r'\s+дом\s+', ' ', res, flags=re.IGNORECASE)
    res = re.sub(r'д\.(\d+)', r'\1', res, flags=re.IGNORECASE)
    res = re.sub(r'дом(\d+)', r'\1', res, flags=re.IGNORECASE)
    
    # Объединение номера дома и корпуса/строения
    res = re.sub(r'(\d+[А-Яа-я]?)\s*[,]?\s*(?:корп\.?|к\.?|к)\s*(\d+)', r'\1к\2', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+[А-Яа-я]?)\s*[,]?\s*(?:стр\.?|строение|с\.?)\s*(\d+)', r'\1 стр. \2', res, flags=re.IGNORECASE)
    res = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', res)
    
    # Разделение названия улицы и номера дома
    res = re.sub(r'([а-яА-ЯёЁ]{2,}(?:\s+[а-яА-ЯёЁ]+){0,3})\s+(\d+[а-яА-Я]?\d*(?:к\d+)?)', r'\1, \2', res)
    
    # Удаление лишних запятых и пробелов
    res = re.sub(r'\s+', ' ', res)
    res = re.sub(r'[,]{2,}', ',', res)
    res = re.sub(r',\s*,', ', ', res)
    res = re.sub(r',\s*(к\d+|стр\.\s*\d+)', r' \1', res)
    res = re.sub(r',\s*д\.\s*$', '', res, flags=re.IGNORECASE)
    res = re.sub(r'ул\.\s+(проспект|пер\.|бульвар|шоссе|набережная|пл\.)', r'\1', res, flags=re.IGNORECASE)
    res = re.sub(r'\b(проспект|ул\.|пер\.|бульвар|шоссе|набережная)\s+д\s*,', r'\1,', res, flags=re.IGNORECASE)
    res = re.sub(r'ул\.\.', 'ул.', res)
    res = re.sub(r',\s*,', ',', res)
    res = re.sub(r'\s+', ' ', res).strip()
    res = re.sub(r'^,\s*', '', res)
    
    # Проверка, что у нас есть улица в адресе
    if re.match(r'^Москва,\s*\d', res):
        match = re.match(r'^Москва,\s*([^,]+)', res)
        if match:
            after_moscow = match.group(1)
            if re.match(r'^\d', after_moscow):
                street_match = re.search(r'([А-Яа-яёЁ]+\s+[А-Яа-яёЁ]+)(?=\s*\d)', raw)
                if street_match:
                    street_name = street_match.group(1)
                    res = f"Москва, ул. {street_name}, {after_moscow}"
    
    return res.strip(' ,.')

# --- TomTom API функции ---
async def geocode_with_tomtom(address: str):
    """Геокодирование адреса через TomTom API"""
    if not TOMTOM_API_KEY:
        return None
    
    url = f"https://api.tomtom.com/search/2/geocode/{address}.json"
    params = {
        "key": TOMTOM_API_KEY,
        "limit": 1,
        "countrySet": "RU",
        "language": "ru-RU"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results') and len(data['results']) > 0:
                        result = data['results'][0]
                        return {
                            'lat': result['position']['lat'],
                            'lon': result['position']['lon'],
                            'address': result['address']['freeformAddress']
                        }
    except Exception as e:
        print(f"TomTom geocoding error: {e}")
    
    return None

async def calculate_route_matrix(origins: list, destinations: list, departure_time: str = None):
    """Расчет матрицы времени/расстояний между точками"""
    if not TOMTOM_API_KEY:
        return None
    
    # Формируем точки в формате TomTom
    origins_str = [f"{o['lon']},{o['lat']}" for o in origins]
    destinations_str = [f"{d['lon']},{d['lat']}" for d in destinations]
    
    url = "https://api.tomtom.com/routing/matrix/2"
    params = {
        "key": TOMTOM_API_KEY,
        "travelMode": "car",
        "traffic": "true",
        "routeType": "fastest"
    }
    
    if departure_time:
        params["departAt"] = departure_time
    
    payload = {
        "origins": [{"point": {"latitude": o['lat'], "longitude": o['lon']}} for o in origins],
        "destinations": [{"point": {"latitude": d['lat'], "longitude": d['lon']}} for d in destinations],
        "options": {
            "traffic": True,
            "travelMode": "car",
            "routeType": "fastest"
        }
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, 
                params=params, 
                json=payload,
                timeout=30
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
    except Exception as e:
        print(f"TomTom matrix error: {e}")
    
    return None

async def calculate_route(waypoints: list, departure_time: str = None, return_to_start: bool = False):
    """Расчет оптимального маршрута через TomTom"""
    if not TOMTOM_API_KEY:
        return None
    
    # Формируем строку точек
    points_str = ";".join([f"{wp['lon']},{wp['lat']}" for wp in waypoints])
    
    url = f"https://api.tomtom.com/routing/1/calculateRoute/{points_str}/json"
    params = {
        "key": TOMTOM_API_KEY,
        "travelMode": "car",
        "traffic": "true",
        "routeType": "fastest",
        "computeBestOrder": True,  # Оптимизация порядка посещения
        "instructionsType": "text",
        "language": "ru-RU"
    }
    
    if departure_time:
        params["departAt"] = departure_time
    
    # Если нужно вернуться в начало, добавляем первую точку в конец
    if return_to_start and len(waypoints) > 1:
        points_str = f"{points_str};{waypoints[0]['lon']},{waypoints[0]['lat']}"
        url = f"https://api.tomtom.com/routing/1/calculateRoute/{points_str}/json"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
    except Exception as e:
        print(f"TomTom routing error: {e}")
    
    return None

# --- Алгоритмы кластеризации ---
def balanced_clustering_by_distance(coords, n_clusters, distance_matrix=None):
    """Сбалансированная кластеризация с учетом матрицы расстояний"""
    n_points = len(coords)
    
    if n_points <= n_clusters:
        labels = list(range(n_points))
        return labels
    
    # Если нет матрицы расстояний, используем географические координаты
    if distance_matrix is None:
        kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
        labels = kmeans.fit_predict(coords)
    else:
        # Используем матрицу расстояний для инициализации центроидов
        # Выбираем самые удаленные точки как начальные центры
        from scipy.spatial.distance import pdist, squareform
        distances = squareform(pdist(coords))
        
        # Первый центр - случайная точка
        centers = [np.random.randint(n_points)]
        
        # Последующие центры - наиболее удаленные от уже выбранных
        for _ in range(1, n_clusters):
            dist_to_centers = distances[:, centers].min(axis=1)
            new_center = np.argmax(dist_to_centers)
            centers.append(new_center)
        
        # Присваиваем точки ближайшему центру
        labels = np.argmin(distances[:, centers], axis=1)
    
    # Балансировка
    labels = balance_clusters(labels, n_clusters)
    
    return labels

def balance_clusters(labels, n_clusters):
    """Балансировка кластеров по количеству точек"""
    n_points = len(labels)
    target_size = n_points // n_clusters
    max_size = target_size + (1 if n_points % n_clusters != 0 else 0)
    
    cluster_sizes = np.bincount(labels, minlength=n_clusters)
    
    for _ in range(100):  # Максимум 100 итераций
        # Находим переполненный и недозаполненный кластеры
        overloaded = np.argmax(cluster_sizes)
        underloaded = np.argmin(cluster_sizes)
        
        if cluster_sizes[overloaded] <= max_size and cluster_sizes[underloaded] >= target_size:
            break
        
        # Находим точку в переполненном кластере, ближайшую к центру недозаполненного
        overloaded_points = np.where(labels == overloaded)[0]
        underloaded_points = np.where(labels == underloaded)[0]
        
        if len(underloaded_points) == 0:
            # Если в недозаполненном кластере нет точек, просто перемещаем случайную
            point_to_move = np.random.choice(overloaded_points)
        else:
            # Вычисляем средние координаты недозаполненного кластера
            underloaded_center = np.mean(underloaded_points)
            
            # Находим ближайшую точку
            distances = np.abs(overloaded_points - underloaded_center)
            point_to_move = overloaded_points[np.argmin(distances)]
        
        # Перемещаем точку
        labels[point_to_move] = underloaded
        cluster_sizes[overloaded] -= 1
        cluster_sizes[underloaded] += 1
    
    return labels

# --- Обработчики команд ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_id = message.from_user.id
    user_data[user_id] = {
        'addresses': [],
        'origin_address': None,
        'origin_coords': None,
        'routes': None,
        'departure_time': None,
        'return_settings': {}
    }
    
    # Кнопка для установки адреса производства
    kb = [[KeyboardButton(text="🏭 Установить адрес производства")]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        "🚚 *Бот логистической компании*\n\n"
        "Отправьте мне PDF-файлы с накладными для извлечения адресов.\n"
        "После загрузки всех файлов нажмите кнопку для распределения маршрутов.\n\n"
        "Сначала установите адрес производства:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(F.text == "🏭 Установить адрес производства")
async def set_origin_address(message: types.Message):
    await message.answer(
        "📍 Отправьте адрес производства (например: Москва, ул. Ленина, 1):\n"
        "Или отправьте геолокацию с телефона."
    )

@dp.message(F.text & ~F.text.startswith('/'))
async def handle_text_address(message: types.Message):
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Проверяем, похоже ли это на адрес
    if any(keyword in text.lower() for keyword in ['москва', 'ул.', 'проспект', 'улица', 'дом', 'д.']):
        # Сохраняем адрес производства
        user_data[user_id]['origin_address'] = text
        
        # Геокодируем через TomTom
        progress = await message.answer("📍 Определяю координаты...")
        
        geocode_result = await geocode_with_tomtom(text)
        if geocode_result:
            user_data[user_id]['origin_coords'] = {
                'lat': geocode_result['lat'],
                'lon': geocode_result['lon'],
                'address': geocode_result['address']
            }
            await progress.delete()
            await message.answer(
                f"✅ Адрес производства установлен:\n"
                f"`{geocode_result['address']}`\n\n"
                f"Координаты: {geocode_result['lat']:.6f}, {geocode_result['lon']:.6f}",
                parse_mode="Markdown"
            )
        else:
            await progress.edit_text("❌ Не удалось определить координаты. Проверьте адрес.")
    else:
        await message.answer("Отправьте мне PDF-файлы с накладными или установите адрес производства.")

@dp.message(F.location)
async def handle_location(message: types.Message):
    user_id = message.from_user.id
    location = message.location
    
    user_data[user_id]['origin_coords'] = {
        'lat': location.latitude,
        'lon': location.longitude,
        'address': f"Геолокация: {location.latitude:.6f}, {location.longitude:.6f}"
    }
    
    await message.answer(
        f"📍 Адрес производства установлен по геолокации:\n"
        f"Координаты: {location.latitude:.6f}, {location.longitude:.6f}",
        parse_mode="Markdown"
    )

@dp.message(F.document)
async def handle_docs(message: types.Message):
    user_id = message.from_user.id
    
    # Инициализация данных пользователя, если нужно
    if user_id not in user_data:
        user_data[user_id] = {
            'addresses': [],
            'origin_address': None,
            'origin_coords': None,
            'routes': None,
            'departure_time': None,
            'return_settings': {}
        }
    
    if not message.document.file_name.lower().endswith('.pdf'): 
        return
    
    # Показываем индикатор загрузки
    processing_msg = await message.answer("📄 *Обработка документа...*", parse_mode="Markdown")
    
    uid = str(uuid.uuid4())
    temp_fn = f"temp_{uid}.pdf"
    try:
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, temp_fn)
        with pdfplumber.open(temp_fn) as pdf:
            text = "".join([p.extract_text() or "" for p in pdf.pages])
            addr = clean_address(text)
            
            # Удаляем сообщение об обработке
            await processing_msg.delete()

            if addr:
                if 'addresses' not in user_data[user_id]:
                    user_data[user_id]['addresses'] = []
                
                # Геокодируем адрес через TomTom
                geocode_msg = await message.answer("📍 Определяю координаты адреса...")
                geocode_result = await geocode_with_tomtom(addr)
                
                if geocode_result:
                    address_data = {
                        'original': addr,
                        'cleaned': geocode_result['address'],
                        'lat': geocode_result['lat'],
                        'lon': geocode_result['lon'],
                        'id': str(uuid.uuid4())[:8]  # Уникальный ID для адреса
                    }
                    user_data[user_id]['addresses'].append(address_data)
                    
                    await geocode_msg.delete()
                    await message.answer(
                        f"✅ *Адрес добавлен:*\n"
                        f"`{geocode_result['address']}`\n\n"
                        f"📍 Координаты: {geocode_result['lat']:.6f}, {geocode_result['lon']:.6f}\n"
                        f"📊 Всего адресов: {len(user_data[user_id]['addresses'])}",
                        parse_mode="Markdown"
                    )
                else:
                    await geocode_msg.edit_text(f"❌ Не удалось определить координаты для адреса:\n`{addr}`")
            else:
                await message.answer(f"❌ Ошибка распознавания в {message.document.file_name}")
    except Exception as e:
        try:
            await processing_msg.delete()
        except:
            pass
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)}")
    finally:
        if os.path.exists(temp_fn): 
            os.remove(temp_fn)
        
        # После обработки файла показываем кнопку для распределения маршрутов
        if user_data[user_id]['addresses']:
            kb = [[KeyboardButton(text="🚚 Распределить маршруты")]]
            markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
            await message.answer(
                f"📊 Загружено адресов: {len(user_data[user_id]['addresses'])}\n"
                f"Нажмите кнопку для распределения маршрутов:",
                reply_markup=markup
            )

@dp.message(F.text == "🚚 Распределить маршруты")
async def start_route_distribution(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Сначала загрузите PDF-файлы с адресами!")
        return
    
    if not user_data[user_id]['origin_coords']:
        await message.answer("❌ Сначала установите адрес производства!")
        return
    
    # Запрашиваем количество водителей
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], 
          [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        f"📊 Всего адресов: {len(user_data[user_id]['addresses'])}\n"
        f"🚚 *На скольких водителей распределить адреса?*",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(F.text.regexp(r'^\d+$'))
async def process_num_drivers(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    num_drivers = int(message.text)
    
    # Сохраняем количество водителей
    user_data[user_id]['num_drivers'] = num_drivers
    
    # Запрашиваем время отправления
    await state.set_state(RouteStates.waiting_for_departure_time)
    
    kb = [[
        KeyboardButton(text="Сейчас"),
        KeyboardButton(text="08:00"),
        KeyboardButton(text="09:00")
    ], [
        KeyboardButton(text="10:00"),
        KeyboardButton(text="Указать свое время")
    ]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        "⏰ *Укажите время отправления водителей:*\n\n"
        "Формат: ЧЧ:MM (например, 08:30)\n"
        "Или выберите из предложенных вариантов.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(RouteStates.waiting_for_departure_time)
async def process_departure_time(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    time_text = message.text.strip()
    
    if time_text == "Указать свое время":
        await message.answer("Введите время в формате ЧЧ:MM (например, 08:30):")
        return
    
    # Парсим время
    if time_text == "Сейчас":
        departure_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        display_time = "текущее время"
    else:
        try:
            # Проверяем формат времени
            if ":" in time_text:
                hours, minutes = map(int, time_text.split(":"))
                if 0 <= hours < 24 and 0 <= minutes < 60:
                    # Используем сегодняшнюю дату
                    today = datetime.now().date()
                    departure_datetime = datetime(
                        today.year, today.month, today.day, 
                        hours, minutes
                    )
                    departure_time = departure_datetime.strftime("%Y-%m-%dT%H:%M:%S")
                    display_time = f"{hours:02d}:{minutes:02d}"
                else:
                    raise ValueError("Неверное время")
            else:
                raise ValueError("Неверный формат")
        except:
            await message.answer("❌ Неверный формат времени. Используйте ЧЧ:MM (например, 08:30)")
            return
    
    user_data[user_id]['departure_time'] = departure_time
    user_data[user_id]['display_time'] = display_time
    
    # Запрашиваем настройки возврата
    await state.set_state(RouteStates.waiting_for_return_settings)
    
    kb = [[
        KeyboardButton(text="Все возвращаются"),
        KeyboardButton(text="Никто не возвращается")
    ], [
        KeyboardButton(text="Указать индивидуально")
    ]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        "🔄 *Настройки возврата на производство:*\n\n"
        "Выберите, какие водители должны вернуться на производство после завершения маршрута.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(RouteStates.waiting_for_return_settings)
async def process_return_settings(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    setting = message.text.strip()
    
    if setting == "Все возвращаются":
        num_drivers = user_data[user_id]['num_drivers']
        user_data[user_id]['return_settings'] = {i: True for i in range(num_drivers)}
        await calculate_and_show_routes(message, state)
    elif setting == "Никто не возвращается":
        num_drivers = user_data[user_id]['num_drivers']
        user_data[user_id]['return_settings'] = {i: False for i in range(num_drivers)}
        await calculate_and_show_routes(message, state)
    elif setting == "Указать индивидуально":
        num_drivers = user_data[user_id]['num_drivers']
        
        # Создаем инлайн-клавиатуру для выбора водителей
        keyboard = []
        for i in range(num_drivers):
            keyboard.append([
                InlineKeyboardButton(
                    text=f"Водитель {i+1} ❌", 
                    callback_data=f"toggle_return_{i}_false"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton(
                text="✅ Продолжить", 
                callback_data="finish_return_settings"
            )
        ])
        
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        
        await message.answer(
            "👥 *Настройка возврата для каждого водителя:*\n\n"
            "Нажмите на кнопку водителя, чтобы изменить настройку возврата.\n"
            "❌ - не возвращается\n"
            "✅ - возвращается\n\n"
            "После настройки нажмите 'Продолжить'.",
            reply_markup=markup,
            parse_mode="Markdown"
        )

@dp.callback_query(F.data.startswith("toggle_return_"))
async def toggle_return_setting(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    
    # Парсим данные
    parts = data.split("_")
    driver_idx = int(parts[2])
    current_setting = parts[3]
    
    # Меняем настройку
    new_setting = "true" if current_setting == "false" else "false"
    
    # Обновляем данные
    if 'return_settings' not in user_data[user_id]:
        user_data[user_id]['return_settings'] = {}
    
    user_data[user_id]['return_settings'][driver_idx] = (new_setting == "true")
    
    # Обновляем кнопку
    button_text = f"Водитель {driver_idx+1} ✅" if new_setting == "true" else f"Водитель {driver_idx+1} ❌"
    
    await callback.message.edit_reply_markup(
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"Водитель {i+1} {'✅' if user_data[user_id]['return_settings'].get(i, False) else '❌'}",
                        callback_data=f"toggle_return_{i}_{str(user_data[user_id]['return_settings'].get(i, False)).lower()}"
                    )
                ] for i in range(user_data[user_id]['num_drivers'])
            ] + [[
                InlineKeyboardButton(
                    text="✅ Продолжить", 
                    callback_data="finish_return_settings"
                )
            ]]
        )
    )
    
    await callback.answer()

@dp.callback_query(F.data == "finish_return_settings")
async def finish_return_settings(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await calculate_and_show_routes(callback.message, state)

async def calculate_and_show_routes(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_data:
        await message.answer("❌ Ошибка: данные не найдены")
        return
    
    # Показываем прогресс
    progress_msg = await message.answer("🗺️ *Строю оптимальные маршруты...*\n\n"
                                       "Это может занять несколько минут.",
                                       parse_mode="Markdown")
    
    try:
        # Подготавливаем данные
        addresses = user_data[user_id]['addresses']
        origin = user_data[user_id]['origin_coords']
        num_drivers = user_data[user_id]['num_drivers']
        departure_time = user_data[user_id]['departure_time']
        return_settings = user_data[user_id]['return_settings']
        
        # Преобразуем координаты в массив
        coords = np.array([[addr['lat'], addr['lon']] for addr in addresses])
        
        # Кластеризуем адреса
        if len(addresses) <= num_drivers:
            # Если адресов меньше или равно количеству водителей
            labels = list(range(len(addresses)))
        else:
            # Используем сбалансированную кластеризацию
            labels = balanced_clustering_by_distance(coords, num_drivers)
        
        # Создаем маршруты для каждого водителя
        routes = []
        total_driving_time = 0
        total_distance = 0
        
        for driver_idx in range(num_drivers):
            # Адреса для этого водителя
            driver_addresses = [addr for i, addr in enumerate(addresses) if labels[i] == driver_idx]
            
            if not driver_addresses:
                # Если у водителя нет адресов
                routes.append({
                    'driver_id': driver_idx,
                    'addresses': [],
                    'waypoints': [],
                    'optimized_route': None,
                    'total_time': 0,
                    'total_distance': 0,
                    'return_to_origin': return_settings.get(driver_idx, True)
                })
                continue
            
            # Формируем waypoints: начало (производство) + адреса водителя
            waypoints = [origin] + driver_addresses
            
            # Рассчитываем маршрут через TomTom
            route_data = await calculate_route(
                waypoints=waypoints,
                departure_time=departure_time,
                return_to_start=return_settings.get(driver_idx, True)
            )
            
            if route_data and 'routes' in route_data and route_data['routes']:
                route_summary = route_data['routes'][0]['summary']
                
                routes.append({
                    'driver_id': driver_idx,
                    'addresses': driver_addresses,
                    'waypoints': waypoints,
                    'optimized_route': route_data,
                    'total_time': route_summary.get('travelTimeInSeconds', 0),
                    'total_distance': route_summary.get('lengthInMeters', 0),
                    'return_to_origin': return_settings.get(driver_idx, True)
                })
                
                total_driving_time += route_summary.get('travelTimeInSeconds', 0)
                total_distance += route_summary.get('lengthInMeters', 0)
            else:
                # Если TomTom API не сработал, создаем простой маршрут
                routes.append({
                    'driver_id': driver_idx,
                    'addresses': driver_addresses,
                    'waypoints': waypoints,
                    'optimized_route': None,
                    'total_time': 0,
                    'total_distance': 0,
                    'return_to_origin': return_settings.get(driver_idx, True)
                })
        
        # Сохраняем маршруты
        user_data[user_id]['routes'] = routes
        user_data[user_id]['clustering_labels'] = labels
        
        # Обновляем сообщение о прогрессе
        await progress_msg.edit_text("✅ *Маршруты построены!*\n\n"
                                    f"⏰ Время отправления: {user_data[user_id]['display_time']}\n"
                                    f"🚚 Количество водителей: {num_drivers}\n"
                                    f"📊 Всего адресов: {len(addresses)}",
                                    parse_mode="Markdown")
        
        # Показываем маршруты
        await show_routes(message, routes, total_driving_time, total_distance)
        
        # Показываем кнопки для редактирования
        await show_edit_buttons(message)
        
    except Exception as e:
        await progress_msg.edit_text(f"❌ Ошибка при построении маршрутов: {str(e)}")
        print(f"Route calculation error: {e}")
    
    await state.clear()

async def show_routes(message: types.Message, routes, total_time, total_distance):
    """Отображение маршрутов"""
    for route in routes:
        driver_idx = route['driver_id'] + 1
        address_count = len(route['addresses'])
        return_text = "🔄 Возвращается на производство" if route['return_to_origin'] else "⏹️ Не возвращается"
        
        # Форматируем время и расстояние
        hours = route['total_time'] // 3600
        minutes = (route['total_time'] % 3600) // 60
        distance_km = route['total_distance'] / 1000
        
        route_text = (
            f"🚛 *МАРШРУТ №{driver_idx}*\n"
            f"📊 Адресов: {address_count}\n"
            f"⏱️ Время: {hours} ч {minutes} мин\n"
            f"📏 Расстояние: {distance_km:.1f} км\n"
            f"{return_text}\n\n"
        )
        
        # Добавляем адреса
        for i, addr in enumerate(route['addresses'], 1):
            route_text += f"{i}. {addr['cleaned']}\n"
        
        await message.answer(route_text, parse_mode="Markdown")
    
    # Общая статистика
    total_hours = total_time // 3600
    total_minutes = (total_time % 3600) // 60
    total_distance_km = total_distance / 1000
    
    stats_text = (
        f"📊 *ОБЩАЯ СТАТИСТИКА*\n"
        f"⏱️ Общее время: {total_hours} ч {total_minutes} мин\n"
        f"📏 Общее расстояние: {total_distance_km:.1f} км\n"
        f"🚚 Количество водителей: {len(routes)}\n"
        f"📍 Всего адресов: {sum(len(r['addresses']) for r in routes)}"
    )
    
    await message.answer(stats_text, parse_mode="Markdown")

async def show_edit_buttons(message: types.Message):
    """Показ кнопок для редактирования маршрутов"""
    keyboard = [
        [InlineKeyboardButton(text="✏️ Редактировать маршруты", callback_data="edit_routes")],
        [InlineKeyboardButton(text="🔄 Пересчитать маршруты", callback_data="recalculate_routes")],
        [InlineKeyboardButton(text="💾 Экспорт маршрутов", callback_data="export_routes")]
    ]
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await message.answer(
        "🛠️ *Управление маршрутами:*\n\n"
        "Вы можете отредактировать маршруты, перемещая адреса между водителями, "
        "или пересчитать маршруты с другими параметрами.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "edit_routes")
async def start_editing_routes(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        await callback.answer("❌ Маршруты не найдены")
        return
    
    routes = user_data[user_id]['routes']
    
    # Создаем меню выбора маршрута для редактирования
    keyboard = []
    for route in routes:
        driver_idx = route['driver_id'] + 1
        address_count = len(route['addresses'])
        keyboard.append([
            InlineKeyboardButton(
                text=f"🚛 Маршрут {driver_idx} ({address_count} адресов)",
                callback_data=f"edit_route_{route['driver_id']}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await callback.message.edit_text(
        "✏️ *Редактирование маршрутов*\n\n"
        "Выберите маршрут для редактирования:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("edit_route_"))
async def edit_specific_route(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    route_idx = int(callback.data.split("_")[2])
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        await callback.answer("❌ Маршруты не найдены")
        return
    
    routes = user_data[user_id]['routes']
    
    if route_idx >= len(routes):
        await callback.answer("❌ Маршрут не найден")
        return
    
    route = routes[route_idx]
    
    # Создаем список адресов с кнопками для перемещения
    keyboard = []
    
    if not route['addresses']:
        keyboard.append([
            InlineKeyboardButton(
                text="⚠️ В этом маршруте нет адресов",
                callback_data="no_action"
            )
        ])
    else:
        for i, addr in enumerate(route['addresses']):
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📍 {addr['cleaned'][:30]}...",
                    callback_data=f"select_address_{route_idx}_{i}"
                )
            ])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="edit_routes")])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await callback.message.edit_text(
        f"🚛 *Редактирование Маршрута №{route_idx + 1}*\n\n"
        f"Выберите адрес для перемещения в другой маршрут:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("select_address_"))
async def select_address_for_moving(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data.split("_")
    route_idx = int(data[2])
    address_idx = int(data[3])
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        await callback.answer("❌ Маршруты не найдены")
        return
    
    # Сохраняем выбранный адрес для перемещения
    await state.set_state(RouteStates.moving_address)
    await state.update_data({
        'moving_from_route': route_idx,
        'moving_address_idx': address_idx
    })
    
    routes = user_data[user_id]['routes']
    
    # Создаем меню выбора целевого маршрута
    keyboard = []
    
    for i, target_route in enumerate(routes):
        if i != route_idx:  # Не показываем текущий маршрут
            driver_idx = i + 1
            address_count = len(target_route['addresses'])
            keyboard.append([
                InlineKeyboardButton(
                    text=f"➡️ Маршрут {driver_idx} ({address_count} адресов)",
                    callback_data=f"move_to_route_{i}"
                )
            ])
    
    keyboard.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"edit_route_{route_idx}")])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await callback.message.edit_text(
        "📤 *Перемещение адреса*\n\n"
        "Выберите маршрут, в который переместить адрес:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("move_to_route_"))
async def move_address_to_route(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    target_route_idx = int(callback.data.split("_")[3])
    
    state_data = await state.get_data()
    source_route_idx = state_data['moving_from_route']
    address_idx = state_data['moving_address_idx']
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        await callback.answer("❌ Маршруты не найдены")
        return
    
    routes = user_data[user_id]['routes']
    
    # Проверяем индексы
    if (source_route_idx >= len(routes) or 
        target_route_idx >= len(routes) or 
        address_idx >= len(routes[source_route_idx]['addresses'])):
        await callback.answer("❌ Ошибка при перемещении")
        return
    
    # Перемещаем адрес
    address_to_move = routes[source_route_idx]['addresses'].pop(address_idx)
    routes[target_route_idx]['addresses'].append(address_to_move)
    
    # Обновляем кластерные метки (для статистики)
    if 'clustering_labels' in user_data[user_id]:
        # Находим индекс адреса в общем списке
        all_addresses = user_data[user_id]['addresses']
        for i, addr in enumerate(all_addresses):
            if addr['id'] == address_to_move['id']:
                user_data[user_id]['clustering_labels'][i] = target_route_idx
                break
    
    # Пересчитываем маршруты
    await recalculate_single_route(callback, source_route_idx)
    await recalculate_single_route(callback, target_route_idx)
    
    await callback.answer(f"✅ Адрес перемещен в Маршрут {target_route_idx + 1}")
    
    # Возвращаемся к редактированию исходного маршрута
    await edit_specific_route(callback, state)

async def recalculate_single_route(callback: CallbackQuery, route_idx: int):
    """Пересчет одного маршрута"""
    user_id = callback.from_user.id
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        return
    
    routes = user_data[user_id]['routes']
    route = routes[route_idx]
    
    # Получаем необходимые данные
    origin = user_data[user_id]['origin_coords']
    departure_time = user_data[user_id]['departure_time']
    return_to_origin = route['return_to_origin']
    
    if not route['addresses']:
        # Если адресов нет
        route['optimized_route'] = None
        route['total_time'] = 0
        route['total_distance'] = 0
        return
    
    # Формируем waypoints
    waypoints = [origin] + route['addresses']
    
    # Рассчитываем маршрут через TomTom
    route_data = await calculate_route(
        waypoints=waypoints,
        departure_time=departure_time,
        return_to_start=return_to_origin
    )
    
    if route_data and 'routes' in route_data and route_data['routes']:
        route_summary = route_data['routes'][0]['summary']
        route['optimized_route'] = route_data
        route['total_time'] = route_summary.get('travelTimeInSeconds', 0)
        route['total_distance'] = route_summary.get('lengthInMeters', 0)
    else:
        route['optimized_route'] = None
        route['total_time'] = 0
        route['total_distance'] = 0

@dp.callback_query(F.data == "recalculate_routes")
async def recalculate_all_routes(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id not in user_data:
        await callback.answer("❌ Данные не найдены")
        return
    
    # Показываем прогресс
    await callback.message.edit_text("🔄 *Пересчитываю маршруты...*", parse_mode="Markdown")
    
    try:
        # Пересчитываем все маршруты
        routes = user_data[user_id]['routes']
        origin = user_data[user_id]['origin_coords']
        departure_time = user_data[user_id]['departure_time']
        
        total_time = 0
        total_distance = 0
        
        for route in routes:
            if not route['addresses']:
                continue
            
            waypoints = [origin] + route['addresses']
            return_to_origin = route['return_to_origin']
            
            route_data = await calculate_route(
                waypoints=waypoints,
                departure_time=departure_time,
                return_to_start=return_to_origin
            )
            
            if route_data and 'routes' in route_data and route_data['routes']:
                route_summary = route_data['routes'][0]['summary']
                route['optimized_route'] = route_data
                route['total_time'] = route_summary.get('travelTimeInSeconds', 0)
                route['total_distance'] = route_summary.get('lengthInMeters', 0)
                
                total_time += route['total_time']
                total_distance += route['total_distance']
        
        # Обновляем сообщение
        await callback.message.edit_text("✅ *Маршруты пересчитаны!*", parse_mode="Markdown")
        
        # Показываем обновленные маршруты
        await show_routes(callback.message, routes, total_time, total_distance)
        
        # Показываем кнопки для редактирования
        await show_edit_buttons(callback.message)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка при пересчете: {str(e)}")

@dp.callback_query(F.data == "export_routes")
async def export_routes(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id not in user_data or 'routes' not in user_data[user_id]:
        await callback.answer("❌ Маршруты не найдены")
        return
    
    routes = user_data[user_id]['routes']
    
    # Создаем текстовый файл с маршрутами
    export_text = "МАРШРУТЫ ДОСТАВКИ\n"
    export_text += f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
    export_text += f"Адрес производства: {user_data[user_id]['origin_coords']['address']}\n"
    export_text += f"Время отправления: {user_data[user_id]['display_time']}\n"
    export_text += "=" * 50 + "\n\n"
    
    for route in routes:
        driver_idx = route['driver_id'] + 1
        hours = route['total_time'] // 3600
        minutes = (route['total_time'] % 3600) // 60
        distance_km = route['total_distance'] / 1000
        return_text = "Возвращается на производство" if route['return_to_origin'] else "Не возвращается"
        
        export_text += f"МАРШРУТ №{driver_idx}\n"
        export_text += f"Адресов: {len(route['addresses'])}\n"
        export_text += f"Время: {hours} ч {minutes} мин\n"
        export_text += f"Расстояние: {distance_km:.1f} км\n"
        export_text += f"{return_text}\n\n"
        
        for i, addr in enumerate(route['addresses'], 1):
            export_text += f"{i}. {addr['cleaned']}\n"
        
        export_text += "\n" + "-" * 40 + "\n\n"
    
    # Отправляем как файл
    await callback.message.answer_document(
        document=types.BufferedInputFile(
            export_text.encode('utf-8'),
            filename=f"маршруты_{datetime.now().strftime('%d%m%Y_%H%M')}.txt"
        ),
        caption="📁 Экспорт маршрутов"
    )

@dp.callback_query(F.data == "back_to_main")
async def back_to_main_menu(callback: CallbackQuery):
    await callback.message.delete()
    await show_edit_buttons(callback.message)

@dp.callback_query(F.data == "no_action")
async def no_action(callback: CallbackQuery):
    await callback.answer()

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
