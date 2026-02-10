import os
import re
import asyncio
import pdfplumber
import pandas as pd
import uuid
import json
import requests
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
from sklearn.cluster import KMeans
import numpy as np
from aiohttp import web

TOKEN = os.getenv("BOT_TOKEN")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Примерная, 1")

bot = Bot(token=TOKEN)
dp = Dispatcher()
user_data = {}

# Состояния для FSM
class RouteStates(StatesGroup):
    waiting_for_drivers = State()
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

# --- TomTom API функции ---
def tomtom_geocode(address):
    """Геокодирование адреса через TomTom API"""
    try:
        url = f"https://api.tomtom.com/search/2/geocode/{address}.json"
        params = {
            'key': TOMTOM_API_KEY,
            'limit': 1,
            'countrySet': 'RU',
            'language': 'ru-RU'
        }
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
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

def tomtom_calculate_route(waypoints, departure_time=None, return_to_start=False):
    """Расчет маршрута через TomTom API с учетом трафика"""
    try:
        # Формируем waypoints: [start, point1, point2, ..., (end)]
        if return_to_start and len(waypoints) > 1:
            # Если возвращаемся к началу, добавляем начальную точку в конец
            waypoints_with_return = waypoints + [waypoints[0]]
            route_points = ";".join([f"{point['lon']},{point['lat']}" for point in waypoints_with_return])
        else:
            route_points = ";".join([f"{point['lon']},{point['lat']}" for point in waypoints])
        
        url = f"https://api.tomtom.com/routing/1/calculateRoute/{route_points}/json"
        
        params = {
            'key': TOMTOM_API_KEY,
            'traffic': 'true',
            'travelMode': 'car',
            'routeType': 'fastest',
            'language': 'ru-RU',
            'instructionsType': 'text',
            'computeBestOrder': 'false',
            'vehicleMaxSpeed': 90,
            'sectionType': 'carTrain'
        }
        
        # Если указано время отправления
        if departure_time:
            params['departAt'] = departure_time.isoformat() + 'Z'
        
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if 'routes' in data and len(data['routes']) > 0:
            route = data['routes'][0]
            summary = route['summary']
            
            # Получаем инструкции для маршрута
            instructions = []
            if 'guidance' in route and 'instructions' in route['guidance']:
                for instruction in route['guidance']['instructions']:
                    if instruction.get('message'):
                        instructions.append(instruction['message'])
            
            return {
                'distance_meters': summary['lengthInMeters'],
                'travel_time_seconds': summary['travelTimeInSeconds'],
                'traffic_delay_seconds': summary.get('trafficDelayInSeconds', 0),
                'departure_time': departure_time.isoformat() if departure_time else None,
                'waypoints': waypoints,
                'instructions': instructions[:10],  # Берем первые 10 инструкций
                'return_to_start': return_to_start
            }
    except Exception as e:
        print(f"TomTom routing error: {e}")
    return None

def optimize_route_order(start_point, points, departure_time=None):
    """Оптимизация порядка точек маршрута (решение задачи коммивояжера)"""
    if len(points) <= 2:
        return [start_point] + points
    
    try:
        # Создаем матрицу точек: start + все точки
        all_points = [start_point] + points
        
        # Строим матрицу расстояний (используем простую эвклидову метрику для оптимизации)
        # В реальном приложении можно использовать TomTom Matrix Routing API
        coords = np.array([[p['lat'], p['lon']] for p in all_points])
        
        # Простой greedy алгоритм для оптимизации порядка
        visited = [0]  # Начинаем с стартовой точки
        unvisited = list(range(1, len(all_points)))
        
        while unvisited:
            last_visited = visited[-1]
            # Находим ближайшую непосещенную точку
            distances = np.linalg.norm(coords[last_visited] - coords[unvisited], axis=1)
            nearest_idx = np.argmin(distances)
            next_point = unvisited[nearest_idx]
            
            visited.append(next_point)
            unvisited.remove(next_point)
        
        # Преобразуем индексы обратно в точки (кроме стартовой)
        ordered_points = [all_points[i] for i in visited[1:]]
        
        return ordered_points
    except Exception as e:
        print(f"Route optimization error: {e}")
        return points  # Возвращаем исходный порядок при ошибке

def balanced_kmeans_clustering(coords, n_clusters, max_iter=100):
    """Сбалансированная кластеризация K-Means"""
    n_points = len(coords)
    
    if n_points <= n_clusters:
        labels = list(range(n_points))
        while len(labels) < n_points:
            labels.append(0)
        return labels
    
    kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
    labels = kmeans.fit_predict(coords)
    
    cluster_sizes = np.bincount(labels, minlength=n_clusters)
    target_size = n_points // n_clusters
    max_per_cluster = target_size + (1 if n_points % n_clusters > 0 else 0)
    
    for iteration in range(max_iter):
        if np.max(cluster_sizes) <= max_per_cluster and np.min(cluster_sizes) >= target_size:
            break
        
        overloaded = np.argmax(cluster_sizes)
        underloaded = np.argmin(cluster_sizes)
        
        if cluster_sizes[overloaded] <= cluster_sizes[underloaded] + 1:
            break
        
        overloaded_points = np.where(labels == overloaded)[0]
        overloaded_coords = coords[overloaded_points]
        underloaded_center = kmeans.cluster_centers_[underloaded]
        
        distances = np.linalg.norm(overloaded_coords - underloaded_center, axis=1)
        idx_to_move = np.argmin(distances)
        point_idx = overloaded_points[idx_to_move]
        
        labels[point_idx] = underloaded
        cluster_sizes[overloaded] -= 1
        cluster_sizes[underloaded] += 1
    
    return labels

# --- Функция очистки адреса (НЕ ТРОГАТЬ!) ---
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

# --- Команды бота ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {
        'addresses': [],
        'processed_files': 0,
        'routes': None,
        'return_to_start': {}  # Словарь: driver_id -> возвращаться ли на базу
    }
    
    keyboard = [
        [KeyboardButton(text="📊 Показать статистику")],
        [KeyboardButton(text="🚚 Распределить адреса")],
        [KeyboardButton(text="🔄 Начать заново")]
    ]
    markup = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        "🚛 *Бот для логистики V2.0*\n\n"
        "📌 *Основные возможности:*\n"
        "• Загрузка PDF-накладных с адресами\n"
        "• Оптимальное распределение по водителям\n"
        "• Построение маршрутов с учетом трафика\n"
        "• Редактирование готовых маршрутов\n\n"
        "📎 *Отправьте PDF-файлы с накладными*",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(Command("help"))
async def help_command(message: types.Message):
    await message.answer(
        "📋 *Помощь по боту:*\n\n"
        "1. *Загрузка файлов:* Отправьте PDF-файлы с накладными\n"
        "2. *Статистика:* Нажмите '📊 Показать статистику' для просмотра загруженных адресов\n"
        "3. *Распределение:* Нажмите '🚚 Распределить адреса' для создания маршрутов\n"
        "4. *Редактирование:* После создания маршрутов можно перемещать адреса между водителями\n"
        "5. *Сброс:* '🔄 Начать заново' очистит все данные\n\n"
        "⏰ *Время отправления:* По умолчанию используется текущее время\n"
        "📍 *Стартовая точка:* Все водители стартуют с адреса производства",
        parse_mode="Markdown"
    )

@dp.message(F.text == "📊 Показать статистику")
async def show_stats(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Нет загруженных адресов")
        return
    
    addresses = user_data[user_id]['addresses']
    unique_addresses = list(set(addresses))
    
    stats = f"📊 *Статистика:*\n\n"
    stats += f"• Всего загружено: {len(addresses)} адрес(ов)\n"
    stats += f"• Уникальных адресов: {len(unique_addresses)}\n"
    stats += f"• Обработано файлов: {user_data[user_id]['processed_files']}\n\n"
    
    if len(unique_addresses) <= 10:
        stats += "📍 *Загруженные адреса:*\n"
        for i, addr in enumerate(unique_addresses[:10], 1):
            short_addr = addr.replace("Москва, ", "")
            stats += f"{i}. {short_addr}\n"
    
    await message.answer(stats, parse_mode="Markdown")

@dp.message(F.text == "🔄 Начать заново")
async def reset_data(message: types.Message):
    user_id = message.from_user.id
    user_data[user_id] = {
        'addresses': [],
        'processed_files': 0,
        'routes': None,
        'return_to_start': {}
    }
    await message.answer("✅ Данные сброшены. Можно загружать новые файлы.")

@dp.message(F.document)
async def handle_docs(message: types.Message):
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
            
            await processing_msg.delete()

            if addr:
                user_id = message.from_user.id
                if user_id not in user_data:
                    user_data[user_id] = {
                        'addresses': [],
                        'processed_files': 0,
                        'routes': None,
                        'return_to_start': {}
                    }
                
                user_data[user_id]['addresses'].append(addr)
                user_data[user_id]['processed_files'] += 1
                
                # Короткая версия адреса для отображения
                short_addr = addr.replace("Москва, ", "")
                
                await message.answer(
                    f"✅ *Файл обработан*\n"
                    f"📄 {message.document.file_name}\n"
                    f"📍 Адрес: {short_addr}\n\n"
                    f"📊 Всего адресов: {len(user_data[user_id]['addresses'])}",
                    parse_mode="Markdown"
                )
            else:
                await message.answer(f"❌ Не удалось извлечь адрес из {message.document.file_name}")
    except Exception as e:
        try:
            await processing_msg.delete()
        except:
            pass
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)[:100]}")
    finally:
        if os.path.exists(temp_fn): 
            os.remove(temp_fn)

@dp.message(F.text == "🚚 Распределить адреса")
async def ask_drivers(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Сначала загрузите PDF-файлы с адресами")
        return
    
    await state.set_state(RouteStates.waiting_for_drivers)
    
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], 
          [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    total_addresses = len(set(user_data[user_id]['addresses']))
    
    await message.answer(
        f"📊 *Готово к распределению*\n"
        f"• Уникальных адресов: {total_addresses}\n"
        f"• Стартовая точка: {PRODUCTION_ADDRESS}\n\n"
        f"🚚 *На скольких водителей распределить адреса?*\n"
        f"(Рекомендуется: {min(6, max(1, total_addresses // 8))}-{min(6, max(2, total_addresses // 4))})",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(RouteStates.waiting_for_drivers, F.text.regexp(r'^\d+$'))
async def process_distribution(message: types.Message, state: FSMContext):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Нет данных для обработки")
        await state.clear()
        return
    
    if num_drivers < 1 or num_drivers > 10:
        await message.answer("❌ Укажите число от 1 до 10")
        return
    
    # Показываем индикатор обработки
    progress_msg = await message.answer(
        "🗺️ *Начинаю построение маршрутов...*\n"
        "⏳ Геокодирование адресов и оптимизация",
        parse_mode="Markdown"
    )
    
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    # 1. Геокодирование всех адресов через TomTom
    await progress_msg.edit_text(
        "🗺️ *Шаг 1/3: Геокодирование адресов...*",
        parse_mode="Markdown"
    )
    
    geo_data = []
    failed_addresses = []
    
    # Геокодируем производственный адрес
    production_geo = tomtom_geocode(PRODUCTION_ADDRESS)
    if not production_geo:
        await progress_msg.edit_text(f"❌ Не удалось геокодировать адрес производства: {PRODUCTION_ADDRESS}")
        await state.clear()
        return
    
    # Геокодируем адреса доставки
    for addr in raw_addresses:
        geo = tomtom_geocode(addr)
        if geo:
            geo_data.append({
                'address': addr,
                'lat': geo['lat'],
                'lon': geo['lon'],
                'geo_address': geo['address']
            })
        else:
            failed_addresses.append(addr)
    
    if len(geo_data) < 2:
        await progress_msg.edit_text("❌ Успешно геокодировано слишком мало адресов")
        await state.clear()
        return
    
    # 2. Кластеризация
    await progress_msg.edit_text(
        "🗺️ *Шаг 2/3: Распределение по водителям...*",
        parse_mode="Markdown"
    )
    
    coords_array = np.array([[item['lat'], item['lon']] for item in geo_data])
    n_cl = min(num_drivers, len(geo_data))
    
    if n_cl > 1:
        labels = balanced_kmeans_clustering(coords_array, n_cl)
    else:
        labels = np.zeros(len(geo_data), dtype=int)
    
    # 3. Построение маршрутов для каждого водителя
    await progress_msg.edit_text(
        "🗺️ *Шаг 3/3: Построение оптимальных маршрутов...*",
        parse_mode="Markdown"
    )
    
    # Сохраняем время отправления (можно сделать настраиваемым)
    departure_time = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
    
    routes = []
    for i in range(n_cl):
        driver_points = [item for j, item in enumerate(geo_data) if labels[j] == i]
        
        if not driver_points:
            continue
        
        # Оптимизируем порядок точек
        start_point = {'lat': production_geo['lat'], 'lon': production_geo['lon']}
        optimized_points = optimize_route_order(start_point, [
            {'lat': p['lat'], 'lon': p['lon']} for p in driver_points
        ])
        
        # По умолчанию для первых двух водителей возвращаемся на базу
        return_to_start = (i < 2)
        
        # Строим маршрут через TomTom
        waypoints = [start_point] + optimized_points
        route_result = tomtom_calculate_route(
            waypoints, 
            departure_time=departure_time,
            return_to_start=return_to_start
        )
        
        if route_result:
            routes.append({
                'driver_id': i,
                'driver_name': f"Водитель {i+1}",
                'addresses': [item['address'] for item in driver_points],
                'geo_data': driver_points,
                'optimized_order': optimized_points,
                'route_info': route_result,
                'return_to_start': return_to_start
            })
    
    # Сохраняем маршруты в user_data
    user_data[user_id]['routes'] = routes
    
    # Обновляем сообщение
    await progress_msg.edit_text(
        "✅ *Маршруты построены!*\n"
        "📋 Выберите действие:",
        parse_mode="Markdown"
    )
    
    # Показываем кнопки для управления
    keyboard = [
        [
            InlineKeyboardButton(text="👁️ Показать маршруты", callback_data="show_routes"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data="edit_routes")
        ],
        [
            InlineKeyboardButton(text="🔄 Изменить возврат на базу", callback_data="toggle_return"),
            InlineKeyboardButton(text="💾 Экспорт", callback_data="export_routes")
        ]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await message.answer(
        f"📊 *Распределение завершено:*\n"
        f"• Водителей: {len(routes)}\n"
        f"• Адресов: {len(raw_addresses)}\n"
        f"• Время отправления: {departure_time.strftime('%H:%M')}\n"
        f"• Необработанных адресов: {len(failed_addresses)}",
        reply_markup=markup,
        parse_mode="Markdown"
    )
    
    await state.clear()

# --- Редактирование маршрутов ---
@dp.callback_query(F.data == "show_routes")
async def show_routes(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    routes = user_data[user_id]['routes']
    
    for i, route in enumerate(routes):
        route_info = route['route_info']
        
        # Форматируем время и расстояние
        travel_time_min = route_info['travel_time_seconds'] // 60
        distance_km = route_info['distance_meters'] / 1000
        
        message_text = f"🚛 *{route['driver_name']}*\n\n"
        message_text += f"📏 *Дистанция:* {distance_km:.1f} км\n"
        message_text += f"⏱️ *Время в пути:* {travel_time_min} мин\n"
        message_text += f"📍 *Точек:* {len(route['addresses'])}\n"
        message_text += f"🔁 *Возврат на базу:* {'Да' if route['return_to_start'] else 'Нет'}\n\n"
        
        if route_info.get('instructions'):
            message_text += "*Основные указания:*\n"
            for j, instr in enumerate(route_info['instructions'][:5], 1):
                message_text += f"{j}. {instr}\n"
        
        message_text += "\n📍 *Адреса:*\n"
        for j, addr in enumerate(route['addresses'], 1):
            short_addr = addr.replace("Москва, ", "")
            message_text += f"{j}. {short_addr}\n"
        
        # Кнопки для редактирования этого конкретного маршрута
        keyboard = []
        for j, addr in enumerate(route['addresses'][:5]):  # Ограничиваем 5 адресами для кнопок
            btn_text = f"{j+1}. {addr.replace('Москва, ', '')[:20]}..."
            keyboard.append([
                InlineKeyboardButton(
                    text=f"➡️ Переместить {btn_text}",
                    callback_data=f"move_{i}_{j}"
                )
            ])
        
        if keyboard:
            markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        else:
            markup = None
        
        await callback.message.answer(message_text, parse_mode="Markdown", reply_markup=markup)
    
    await callback.answer()

@dp.callback_query(F.data == "edit_routes")
async def edit_routes(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    await state.set_state(RouteStates.editing_routes)
    
    routes = user_data[user_id]['routes']
    
    # Создаем обзорную таблицу маршрутов
    message_text = "✏️ *Режим редактирования маршрутов*\n\n"
    
    for i, route in enumerate(routes):
        message_text += f"🚛 *{route['driver_name']}*\n"
        message_text += f"   📍 Адресов: {len(route['addresses'])}\n"
        message_text += f"   🔁 Возврат: {'Да' if route['return_to_start'] else 'Нет'}\n\n"
    
    # Кнопки для выбора действия
    keyboard = []
    for i, route in enumerate(routes):
        keyboard.append([
            InlineKeyboardButton(
                text=f"👁️ Показать {route['driver_name']}",
                callback_data=f"view_route_{i}"
            ),
            InlineKeyboardButton(
                text=f"✏️ Редактировать {route['driver_name']}",
                callback_data=f"edit_route_{i}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="✅ Завершить редактирование", callback_data="finish_edit")
    ])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await callback.message.edit_text(message_text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(F.data.startswith("move_"))
async def move_address(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    # Парсим данные: move_routeIndex_addressIndex
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ Ошибка в данных")
        return
    
    route_index = int(parts[1])
    address_index = int(parts[2])
    
    routes = user_data[user_id]['routes']
    
    if route_index >= len(routes) or address_index >= len(routes[route_index]['addresses']):
        await callback.answer("❌ Некорректный индекс")
        return
    
    # Сохраняем информацию о перемещении
    await state.update_data({
        'moving_from_route': route_index,
        'moving_address_index': address_index,
        'moving_address': routes[route_index]['addresses'][address_index]
    })
    
    await state.set_state(RouteStates.moving_address)
    
    # Создаем кнопки для выбора целевого маршрута
    keyboard = []
    for i, route in enumerate(routes):
        if i != route_index:  # Не показываем текущий маршрут
            keyboard.append([
                InlineKeyboardButton(
                    text=f"➡️ В {route['driver_name']} ({len(route['addresses'])} адр.)",
                    callback_data=f"to_route_{i}"
                )
            ])
    
    keyboard.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_move")
    ])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    address_to_move = routes[route_index]['addresses'][address_index]
    short_addr = address_to_move.replace("Москва, ", "")
    
    await callback.message.edit_text(
        f"✏️ *Перемещение адреса:*\n{short_addr}\n\n"
        f"*Выберите целевой маршрут:*",
        parse_mode="Markdown",
        reply_markup=markup
    )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("to_route_"))
async def confirm_move(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    # Парсим целевой маршрут
    target_route_index = int(callback.data.split("_")[2])
    
    # Получаем данные о перемещении
    state_data = await state.get_data()
    source_route_index = state_data['moving_from_route']
    address_index = state_data['moving_address_index']
    address = state_data['moving_address']
    
    routes = user_data[user_id]['routes']
    
    # Проверяем индексы
    if (source_route_index >= len(routes) or 
        target_route_index >= len(routes) or 
        address_index >= len(routes[source_route_index]['addresses'])):
        await callback.answer("❌ Некорректные индексы")
        return
    
    # Перемещаем адрес
    address_to_move = routes[source_route_index]['addresses'].pop(address_index)
    
    # Также нужно удалить соответствующие геоданные
    if address_index < len(routes[source_route_index]['geo_data']):
        geo_data_to_move = routes[source_route_index]['geo_data'].pop(address_index)
        routes[target_route_index]['geo_data'].append(geo_data_to_move)
    
    routes[target_route_index]['addresses'].append(address_to_move)
    
    # Обновляем маршруты в user_data
    user_data[user_id]['routes'] = routes
    
    short_addr = address_to_move.replace("Москва, ", "")
    
    await callback.message.edit_text(
        f"✅ *Адрес перемещен!*\n"
        f"📍 {short_addr}\n"
        f"📤 Из: {routes[source_route_index]['driver_name']}\n"
        f"📥 В: {routes[target_route_index]['driver_name']}",
        parse_mode="Markdown"
    )
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "cancel_move")
async def cancel_move(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Перемещение отменено")
    await callback.answer()

@dp.callback_query(F.data == "toggle_return")
async def toggle_return(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    routes = user_data[user_id]['routes']
    
    # Создаем кнопки для переключения возврата на базу
    keyboard = []
    for i, route in enumerate(routes):
        current_status = "🔴" if not route['return_to_start'] else "🟢"
        keyboard.append([
            InlineKeyboardButton(
                text=f"{current_status} {route['driver_name']} - Возврат: {'Да' if route['return_to_start'] else 'Нет'}",
                callback_data=f"toggle_{i}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="✅ Применить изменения", callback_data="apply_return_changes")
    ])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await callback.message.edit_text(
        "🔄 *Настройка возврата на базу*\n\n"
        "🟢 - возвращается на базу\n"
        "🔴 - не возвращается\n\n"
        "Выберите водителей для изменения:",
        parse_mode="Markdown",
        reply_markup=markup
    )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_"))
async def toggle_single_return(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    route_index = int(callback.data.split("_")[1])
    routes = user_data[user_id]['routes']
    
    if route_index < len(routes):
        # Переключаем статус
        routes[route_index]['return_to_start'] = not routes[route_index]['return_to_start']
        
        # Обновляем сообщение
        await toggle_return(callback)
    
    await callback.answer()

@dp.callback_query(F.data == "apply_return_changes")
async def apply_return_changes(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    # Здесь можно перестроить маршруты с новыми настройками возврата
    # Для простоты пока просто сохраняем настройки
    
    await callback.message.edit_text(
        "✅ *Настройки возврата сохранены!*\n\n"
        "Для перестроения маршрутов с новыми параметрами "
        "используйте команду распределения заново.",
        parse_mode="Markdown"
    )
    
    await callback.answer()

@dp.callback_query(F.data == "export_routes")
async def export_routes(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_data or not user_data[user_id].get('routes'):
        await callback.answer("❌ Нет доступных маршрутов")
        return
    
    routes = user_data[user_id]['routes']
    
    # Создаем текстовый экспорт
    export_text = "📋 Экспорт маршрутов\n\n"
    
    for route in routes:
        export_text += f"🚛 {route['driver_name']}\n"
        export_text += f"Возврат на базу: {'Да' if route['return_to_start'] else 'Нет'}\n"
        export_text += f"Адресов: {len(route['addresses'])}\n\n"
        
        for i, addr in enumerate(route['addresses'], 1):
            short_addr = addr.replace("Москва, ", "")
            export_text += f"{i}. {short_addr}\n"
        
        export_text += "\n" + "="*50 + "\n\n"
    
    # Сохраняем в файл
    filename = f"routes_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(export_text)
    
    # Отправляем файл пользователю
    with open(filename, 'rb') as f:
        await callback.message.answer_document(
            types.FSInputFile(filename),
            caption="📎 Экспорт маршрутов"
        )
    
    # Удаляем временный файл
    os.remove(filename)
    
    await callback.answer()

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
