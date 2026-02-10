import os
import re
import asyncio
import json
import uuid
import pdfplumber
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, InlineKeyboardMarkup
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from sklearn.cluster import KMeans
from geopy.geocoders import Nominatim
from aiohttp import web

# Загрузка конфигурации
TOKEN = os.getenv("BOT_TOKEN")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
PRODUCTION_ADDRESS = os.getenv("PRODUCTION_ADDRESS", "Москва, ул. Лавочкина, 34")

# Инициализация
storage = MemoryStorage()
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=storage)

# Хранение данных пользователей
user_data: Dict[int, Dict] = {}

# Состояния для FSM
class DistributionStates(StatesGroup):
    waiting_for_drivers = State()
    waiting_for_departure_time = State()
    setting_return_to_base = State()

class EditRoutesStates(StatesGroup):
    selecting_route = State()
    selecting_address = State()
    selecting_target_route = State()

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

# --- ФУНКЦИЯ ОЧИСТКИ АДРЕСА (НЕ МЕНЯТЬ!) ---
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

# --- Геокодирование через несколько сервисов ---
async def geocode_with_fallback(address: str) -> Optional[Tuple[float, float]]:
    """Геокодирование через TomTom, с fallback на Nominatim"""
    coords = await tomtom_geocode(address)
    if coords:
        return coords
    
    coords = await nominatim_geocode(address)
    return coords

async def tomtom_geocode(address: str) -> Optional[Tuple[float, float]]:
    """Геокодирование адреса с помощью TomTom API"""
    try:
        encoded_address = aiohttp.helpers.quote(address)
        url = f"https://api.tomtom.com/search/2/geocode/{encoded_address}.json"
        params = {
            "key": TOMTOM_API_KEY,
            "limit": 1,
            "countrySet": "RU",
            "language": "ru-RU",
            "typeahead": "false"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("results") and len(data["results"]) > 0:
                        position = data["results"][0]["position"]
                        return (position["lat"], position["lon"])
        return None
    except Exception:
        return None

async def nominatim_geocode(address: str) -> Optional[Tuple[float, float]]:
    """Геокодирование через Nominatim как fallback"""
    try:
        if "Москва" not in address:
            address_to_geocode = f"Москва, {address}"
        else:
            address_to_geocode = address
            
        geolocator = Nominatim(user_agent="logistics_bot_v4", timeout=10)
        location = geolocator.geocode(address_to_geocode)
        if location:
            return (location.latitude, location.longitude)
        return None
    except Exception:
        return None

async def batch_geocode(addresses: List[str]) -> Tuple[Dict[str, Tuple[float, float]], List[str]]:
    """Пакетное геокодирование адресов"""
    coords_dict = {}
    failed_addresses = []
    
    for i, address in enumerate(addresses):
        coords = await geocode_with_fallback(address)
        if coords:
            coords_dict[address] = coords
        else:
            failed_addresses.append(address)
        await asyncio.sleep(0.2)
    
    return coords_dict, failed_addresses

# --- TomTom Routing API с оптимизацией порядка ---
async def tomtom_calculate_optimized_route(waypoints: List[Tuple[float, float]], 
                                          departure_time: Optional[str] = None,
                                          return_to_start: bool = False) -> Dict:
    """Расчет оптимального маршрута с оптимизацией порядка точек"""
    try:
        if len(waypoints) < 2:
            return {}
        
        # Если требуется возврат, добавляем стартовую точку в конец
        if return_to_start:
            final_waypoints = waypoints.copy()
        else:
            final_waypoints = waypoints
        
        # Форматируем waypoints для API
        waypoints_str = ":".join([f"{lat},{lon}" for lat, lon in final_waypoints])
        
        url = f"https://api.tomtom.com/routing/1/calculateRoute/{waypoints_str}/json"
        params = {
            "key": TOMTOM_API_KEY,
            "travelMode": "truck",
            "vehicleMaxSpeed": 90,
            "vehicleWeight": 3500,
            "vehicleLength": 6,
            "vehicleWidth": 2.5,
            "vehicleHeight": 3.5,
            "routeType": "fastest",
            "traffic": "true",
            "computeBestOrder": "true",  # Оптимизация порядка точек
            "instructionsType": "text",
            "language": "ru-RU",
            "vehicleCommercial": "true",
            "vehicleLoadType": "generalGoods",
            "avoid": "unpavedRoads"
        }
        
        if departure_time:
            try:
                if "T" not in departure_time:
                    departure_dt = datetime.fromisoformat(departure_time)
                    departure_time = departure_dt.isoformat()
                params["departAt"] = departure_time
            except:
                pass
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Извлекаем оптимизированный порядок точек
                    if data.get("optimizedWaypoints"):
                        optimized_order = [wp["optimizedIndex"] for wp in data["optimizedWaypoints"]]
                        data["optimizedOrder"] = optimized_order
                    
                    return data
                else:
                    return {}
    except Exception:
        return {}

def optimize_route_nearest_neighbor(start_coords: Tuple[float, float], 
                                   points: List[Tuple[str, Tuple[float, float]]]) -> List[str]:
    """Оптимизация маршрута алгоритмом ближайшего соседа"""
    if not points:
        return []
    
    # Преобразуем в numpy для вычислений
    import numpy as np
    
    point_coords = np.array([coord for _, coord in points])
    point_addresses = [addr for addr, _ in points]
    
    unvisited = set(range(len(points)))
    current_idx = None
    current_coords = np.array(start_coords)
    route_order = []
    
    while unvisited:
        if current_idx is not None:
            unvisited.remove(current_idx)
        
        # Находим ближайшую непосещенную точку
        min_dist = float('inf')
        next_idx = None
        
        for idx in unvisited:
            dist = np.linalg.norm(current_coords - point_coords[idx])
            if dist < min_dist:
                min_dist = dist
                next_idx = idx
        
        if next_idx is not None:
            route_order.append(point_addresses[next_idx])
            current_coords = point_coords[next_idx]
            current_idx = next_idx
        else:
            break
    
    return route_order

# --- Алгоритмы балансировки маршрутов ---
def balanced_clustering(coords_dict: Dict[str, Tuple[float, float]], 
                       n_clusters: int,
                       production_coords: Tuple[float, float]) -> Dict[int, List[str]]:
    """Сбалансированная кластеризация с учетом географии"""
    addresses = list(coords_dict.keys())
    coords = np.array([coords_dict[addr] for addr in addresses])
    
    if len(addresses) <= n_clusters:
        result = {}
        for i in range(n_clusters):
            result[i] = addresses[i:i+1] if i < len(addresses) else []
        return result
    
    kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
    labels = kmeans.fit_predict(coords)
    
    cluster_sizes = np.bincount(labels, minlength=n_clusters)
    target_size = len(addresses) // n_clusters
    max_size = target_size + (1 if len(addresses) % n_clusters else 0)
    
    for _ in range(100):
        max_cluster = np.argmax(cluster_sizes)
        min_cluster = np.argmin(cluster_sizes)
        
        if cluster_sizes[max_cluster] <= max_size and cluster_sizes[min_cluster] >= target_size:
            break
        
        if cluster_sizes[max_cluster] > max_size:
            max_cluster_points = np.where(labels == max_cluster)[0]
            max_cluster_coords = coords[max_cluster_points]
            min_cluster_center = kmeans.cluster_centers_[min_cluster]
            
            distances = np.linalg.norm(max_cluster_coords - min_cluster_center, axis=1)
            idx_to_move = np.argmin(distances)
            point_idx = max_cluster_points[idx_to_move]
            
            labels[point_idx] = min_cluster
            cluster_sizes[max_cluster] -= 1
            cluster_sizes[min_cluster] += 1
    
    result = {}
    for i in range(n_clusters):
        cluster_addresses = [addresses[j] for j in range(len(addresses)) if labels[j] == i]
        result[i] = cluster_addresses
    
    return result

# --- Основное меню ---
def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню с кнопками"""
    keyboard = [
        [KeyboardButton(text="📁 Загрузить PDF")],
        [KeyboardButton(text="🚚 Распределить адреса")],
        [KeyboardButton(text="✏️ Редактировать маршруты")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📤 Экспорт")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# --- Обработчики команд и кнопок ---
@dp.message(Command("start"))
async def start(message: types.Message):
    user_id = message.from_user.id
    user_data[user_id] = {
        'addresses': [],
        'processed_files': 0,
        'routes_info': None,
        'address_coords': {},
        'production_coords': None,
        'departure_time': None,
        'return_to_base': {},
        'need_return_config': False
    }
    
    await message.answer(
        "🚛 *Логистический бот V4.0* 🚛\n\n"
        "📋 *Улучшенные возможности:*\n"
        "• Оптимальный порядок адресов в маршрутах\n"
        "• Удобное редактирование через кнопки\n"
        "• Настройка возврата на базу\n"
        "• Кнопочное меню для всех действий\n\n"
        "📎 *Используйте кнопки ниже для работы:*",
        reply_markup=get_main_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📁 Загрузить PDF")
async def handle_load_pdf(message: types.Message):
    await message.answer(
        "📁 *Загрузка PDF-файлов*\n\n"
        "Отправьте мне PDF-файлы с накладными.\n"
        "После загрузки всех файлов нажмите кнопку '🚚 Распределить адреса'.",
        reply_markup=get_main_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'): 
        return
    
    user_id = message.from_user.id
    if user_id not in user_data:
        user_data[user_id] = {
            'addresses': [],
            'processed_files': 0,
            'routes_info': None,
            'address_coords': {},
            'production_coords': None,
            'departure_time': None,
            'return_to_base': {},
            'need_return_config': False
        }
    
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
                if addr not in user_data[user_id]['addresses']:
                    user_data[user_id]['addresses'].append(addr)
                
                user_data[user_id]['processed_files'] += 1
                
                total_addresses = len(user_data[user_id]['addresses'])
                total_files = user_data[user_id]['processed_files']
                
                await message.answer(
                    f"✅ *Файл обработан:* {message.document.file_name}\n"
                    f"📍 *Адрес:* {addr}\n\n"
                    f"📊 *Статистика:*\n"
                    f"• Обработано файлов: {total_files}\n"
                    f"• Уникальных адресов: {total_addresses}\n\n"
                    f"📎 Отправьте следующий файл или нажмите '🚚 Распределить адреса'",
                    reply_markup=get_main_keyboard(),
                    parse_mode="Markdown"
                )
            else:
                await message.answer(f"❌ Ошибка распознавания адреса в {message.document.file_name}",
                                   reply_markup=get_main_keyboard())
    except Exception as e:
        try:
            await processing_msg.delete()
        except:
            pass
        await message.answer(f"❌ Ошибка обработки файла: {str(e)}",
                           reply_markup=get_main_keyboard())
    finally:
        if os.path.exists(temp_fn): 
            os.remove(temp_fn)

@dp.message(F.text == "🚚 Распределить адреса")
async def handle_distribute(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Нет адресов для распределения. Сначала загрузите PDF-файлы.",
                           reply_markup=get_main_keyboard())
        return
    
    addresses = user_data[user_id]['addresses']
    await message.answer(
        f"📊 *Готово к распределению!*\n"
        f"• Всего адресов: {len(addresses)}\n"
        f"• Адрес производства: {PRODUCTION_ADDRESS}\n\n"
        f"🚚 *Введите количество водителей (1-10):*",
        parse_mode="Markdown"
    )
    await state.set_state(DistributionStates.waiting_for_drivers)

@dp.message(DistributionStates.waiting_for_drivers)
async def process_drivers_count(message: types.Message, state: FSMContext):
    try:
        num_drivers = int(message.text)
        if num_drivers < 1 or num_drivers > 10:
            await message.answer("❌ Введите число от 1 до 10")
            return
        
        user_id = message.from_user.id
        user_data[user_id]['num_drivers'] = num_drivers
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⏱ Сейчас")],
                [KeyboardButton(text="🕗 08:00")],
                [KeyboardButton(text="🕘 09:00")],
                [KeyboardButton(text="🕙 10:00")],
                [KeyboardButton(text="✏️ Ввести вручную")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "⏰ *Выберите время отправления водителей:*\n\n"
            "• ⏱ Сейчас - текущее время\n"
            "• Или выберите из предложенных\n"
            "• Или введите время в формате ЧЧ:ММ (например, 08:30)",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        await state.set_state(DistributionStates.waiting_for_departure_time)
        
    except ValueError:
        await message.answer("❌ Введите корректное число")

@dp.message(DistributionStates.waiting_for_departure_time)
async def process_departure_time(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if message.text in ["⏱ Сейчас", "Сейчас"]:
        departure_time = datetime.now().isoformat()
    elif message.text == "✏️ Ввести вручную":
        await message.answer("⏰ Введите время в формате ЧЧ:ММ (например, 08:30):")
        return
    elif any(msg in message.text for msg in ["🕗", "🕘", "🕙"]):
        time_map = {"🕗 08:00": "08:00", "🕘 09:00": "09:00", "🕙 10:00": "10:00"}
        time_str = time_map.get(message.text, "08:00")
        hours, minutes = map(int, time_str.split(':'))
        now = datetime.now()
        departure_time = datetime(now.year, now.month, now.day, hours, minutes).isoformat()
    elif re.match(r'^\d{1,2}:\d{2}$', message.text):
        hours, minutes = map(int, message.text.split(':'))
        now = datetime.now()
        departure_time = datetime(now.year, now.month, now.day, hours, minutes).isoformat()
    else:
        try:
            departure_time = datetime.fromisoformat(message.text).isoformat()
        except:
            await message.answer("❌ Неверный формат времени. Используйте ЧЧ:ММ")
            return
    
    user_data[user_id]['departure_time'] = departure_time
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Да, настроить возврат")],
            [KeyboardButton(text="❌ Нет, без возврата")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔄 *Настройка возврата на базу:*\n\n"
        "Хотите настроить, какие водители возвращаются на производство после завершения маршрута?\n\n"
        "✅ *Да* - сможете указать для каждого водителя отдельно\n"
        "❌ *Нет* - все водители не возвращаются на базу",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await state.set_state(DistributionStates.setting_return_to_base)

@dp.message(DistributionStates.setting_return_to_base)
async def process_return_setup(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if message.text == "✅ Да, настроить возврат":
        user_data[user_id]['need_return_config'] = True
        await message.answer("✅ Возврат на базу будет настроен после распределения адресов.")
    else:
        user_data[user_id]['need_return_config'] = False
        await message.answer("❌ Возврат на базу не настроен. Все водители завершают маршрут на последнем адресе.")
    
    await process_distribution(message, state)

async def process_distribution(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    progress_msg = await message.answer("🔄 *Начинаю обработку...*\n1️⃣ Геокодирование адресов", parse_mode="Markdown")
    
    # Геокодирование производства
    await progress_msg.edit_text("📍 *Геокодирование адреса производства...*")
    production_coords = await geocode_with_fallback(PRODUCTION_ADDRESS)
    if not production_coords:
        await progress_msg.edit_text("❌ Не удалось определить координаты производства")
        await state.clear()
        return
    
    user_data[user_id]['production_coords'] = production_coords
    
    # Геокодирование адресов доставки
    addresses = list(set(user_data[user_id]['addresses']))
    await progress_msg.edit_text(f"📍 *Геокодирование {len(addresses)} адресов доставки...*\n⏳ Это может занять время")
    
    coords_dict, failed_addresses = await batch_geocode(addresses)
    
    if failed_addresses:
        failed_text = "\n".join([f"• {addr.replace('Москва, ', '')}" for addr in failed_addresses])
        await message.answer(
            f"⚠️ *Не удалось геокодировать {len(failed_addresses)} адресов:*\n\n{failed_text}\n\n"
            f"Эти адреса не будут включены в распределение.",
            parse_mode="Markdown"
        )
    
    if not coords_dict:
        await progress_msg.edit_text("❌ Не удалось геокодировать ни один адрес доставки")
        await state.clear()
        return
    
    user_data[user_id]['address_coords'] = coords_dict
    
    await progress_msg.edit_text(f"✅ Геокодирование завершено\n📍 Успешно: {len(coords_dict)} из {len(addresses)} адресов")
    
    # Балансировка и кластеризация
    await progress_msg.edit_text("🔄 *Распределение адресов между водителями...*")
    
    num_drivers = user_data[user_id]['num_drivers']
    clusters = balanced_clustering(coords_dict, num_drivers, production_coords)
    
    # Расчет маршрутов с оптимизацией порядка
    await progress_msg.edit_text("🔄 *Расчет оптимальных маршрутов...*\n⏳ Учитываю трафик, время и оптимизирую порядок")
    
    routes_info = {}
    departure_time = user_data[user_id]['departure_time']
    
    for driver_id, driver_addresses in clusters.items():
        if driver_addresses:
            # Формируем список точек для маршрута
            points = [(addr, coords_dict[addr]) for addr in driver_addresses if addr in coords_dict]
            
            # Оптимизируем порядок адресов
            optimized_order = optimize_route_nearest_neighbor(production_coords, points)
            
            # Формируем waypoints в оптимальном порядке
            waypoints = [production_coords]
            for addr in optimized_order:
                if addr in coords_dict:
                    waypoints.append(coords_dict[addr])
            
            # Рассчитываем маршрут через TomTom
            route_data = await tomtom_calculate_optimized_route(
                waypoints, 
                departure_time,
                return_to_start=False
            )
            
            routes_info[driver_id] = {
                'addresses': optimized_order,  # Сохраняем оптимизированный порядок
                'original_addresses': driver_addresses,
                'route_data': route_data,
                'waypoints': waypoints,
                'return_to_base': False
            }
        else:
            routes_info[driver_id] = {
                'addresses': [],
                'original_addresses': [],
                'route_data': {},
                'waypoints': [production_coords],
                'return_to_base': False
            }
    
    user_data[user_id]['routes_info'] = routes_info
    
    # Показываем результаты
    await progress_msg.delete()
    await show_routes(message, user_id)
    
    # Если нужно настроить возврат на базу
    if user_data[user_id].get('need_return_config'):
        await setup_return_to_base(message, user_id)
    else:
        await offer_actions(message, user_id)
    
    await state.clear()

async def show_routes(message: types.Message, user_id: int):
    """Показать построенные маршруты"""
    routes_info = user_data[user_id]['routes_info']
    
    for driver_id, info in sorted(routes_info.items()):
        route_data = info.get('route_data', {})
        addresses = info['addresses']
        
        summary = route_data.get('routes', [{}])[0].get('summary', {})
        total_time = summary.get('travelTimeInSeconds', 0)
        total_distance = summary.get('lengthInMeters', 0)
        
        route_text = f"🚛 *МАРШРУТ {driver_id+1}*\n"
        
        if total_time > 0:
            route_text += f"⏱ Время: {total_time // 60} мин\n"
        if total_distance > 0:
            route_text += f"📏 Расстояние: {total_distance / 1000:.1f} км\n"
        
        route_text += f"📍 Адресов: {len(addresses)}\n"
        
        if info.get('return_to_base'):
            route_text += f"🔄 Возврат на базу: ✅\n"
        
        route_text += "\n*Порядок объезда:*\n"
        
        # Добавляем адреса в оптимальном порядке
        for i, addr in enumerate(addresses, 1):
            short_addr = addr.replace("Москва, ", "")
            route_text += f"{i}. {short_addr}\n"
        
        await message.answer(route_text, parse_mode="Markdown")
    
    # Показываем статистику
    await show_distribution_stats(message, user_id)

async def show_distribution_stats(message: types.Message, user_id: int):
    """Показать статистику распределения"""
    routes_info = user_data[user_id]['routes_info']
    all_addresses = user_data[user_id]['addresses']
    
    stats_text = "📊 *Статистика распределения:*\n\n"
    total_distributed = 0
    total_time = 0
    total_distance = 0
    
    for driver_id, info in sorted(routes_info.items()):
        addresses = info['addresses']
        total_distributed += len(addresses)
        
        stats_text += f"🚛 *Маршрут {driver_id+1}:*\n"
        stats_text += f"   📍 Адресов: {len(addresses)}\n"
        
        route_data = info.get('route_data', {})
        if route_data:
            summary = route_data.get('routes', [{}])[0].get('summary', {})
            travel_time = summary.get('travelTimeInSeconds', 0) // 60
            distance = summary.get('lengthInMeters', 0) / 1000
            total_time += travel_time
            total_distance += distance
            
            if travel_time > 0:
                stats_text += f"   ⏱ Время: {travel_time} мин\n"
            if distance > 0:
                stats_text += f"   📏 Расстояние: {distance:.1f} км\n"
        
        if info.get('return_to_base'):
            stats_text += f"   🔄 Возврат на базу: ✅\n"
        
        stats_text += "\n"
    
    stats_text += f"📈 *Итого:*\n"
    stats_text += f"   📍 Всего адресов: {len(all_addresses)}\n"
    stats_text += f"   📍 Распределено: {total_distributed}\n"
    stats_text += f"   📍 Не распределено: {len(all_addresses) - total_distributed}\n"
    
    if total_time > 0:
        stats_text += f"   ⏱ Общее время: {total_time} мин\n"
    if total_distance > 0:
        stats_text += f"   📏 Общее расстояние: {total_distance:.1f} км\n"
    
    stats_text += f"   🚛 Водителей: {len(routes_info)}"
    
    await message.answer(stats_text, parse_mode="Markdown", reply_markup=get_main_keyboard())

async def setup_return_to_base(message: types.Message, user_id: int):
    """Настройка возврата на базу для водителей"""
    routes_info = user_data[user_id]['routes_info']
    
    # Создаем инлайн-клавиатуру для настройки возврата
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{'✅' if info.get('return_to_base') else '❌'} Маршрут {driver_id+1} ({len(info['addresses'])} адр.)",
            callback_data=f"toggle_return_{driver_id}"
        )] for driver_id, info in sorted(routes_info.items())
    ] + [
        [InlineKeyboardButton(text="🚀 Завершить настройку", callback_data="finish_return_setup")],
        [InlineKeyboardButton(text="📊 Показать маршруты", callback_data="show_routes_again")]
    ])
    
    await message.answer(
        "🔄 *Настройка возврата на базу:*\n\n"
        "Выберите, какие водители должны вернуться на производство после завершения маршрута.\n"
        "Нажмите на кнопку маршрута, чтобы переключить состояние возврата.\n\n"
        "✅ - водитель возвращается на базу\n"
        "❌ - водитель не возвращается",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("toggle_return_"))
async def toggle_return_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    driver_id = int(callback.data.split("_")[-1])
    
    if user_id in user_data and driver_id in user_data[user_id]['routes_info']:
        current = user_data[user_id]['routes_info'][driver_id].get('return_to_base', False)
        user_data[user_id]['routes_info'][driver_id]['return_to_base'] = not current
        
        routes_info = user_data[user_id]['routes_info']
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"{'✅' if info.get('return_to_base') else '❌'} Маршрут {dr_id+1} ({len(info['addresses'])} адр.)",
                callback_data=f"toggle_return_{dr_id}"
            )] for dr_id, info in sorted(routes_info.items())
        ] + [
            [InlineKeyboardButton(text="🚀 Завершить настройку", callback_data="finish_return_setup")],
            [InlineKeyboardButton(text="📊 Показать маршруты", callback_data="show_routes_again")]
        ])
        
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        status = "возвращается" if not current else "не возвращается"
        await callback.answer(f"Маршрут {driver_id+1}: {status} на базу")

@dp.callback_query(F.data == "show_routes_again")
async def show_routes_again_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    await show_routes(callback.message, user_id)
    await callback.answer()

@dp.callback_query(F.data == "finish_return_setup")
async def finish_return_setup_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    # Пересчитываем маршруты для тех, у кого включен возврат
    routes_info = user_data[user_id]['routes_info']
    address_coords = user_data[user_id]['address_coords']
    production_coords = user_data[user_id]['production_coords']
    departure_time = user_data[user_id]['departure_time']
    
    await callback.message.edit_text("🔄 Пересчитываю маршруты с учетом возврата на базу...")
    
    for driver_id, info in routes_info.items():
        if info.get('return_to_base') and info['addresses']:
            waypoints = [production_coords]
            for addr in info['addresses']:
                if addr in address_coords:
                    waypoints.append(address_coords[addr])
            
            route_data = await tomtom_calculate_optimized_route(
                waypoints, 
                departure_time,
                return_to_start=True
            )
            
            info['route_data'] = route_data
            info['waypoints'] = waypoints
    
    await callback.message.answer("✅ Настройка возврата завершена. Маршруты пересчитаны.")
    await show_routes(callback.message, user_id)
    await offer_actions(callback.message, user_id)
    await callback.answer()

async def offer_actions(message: types.Message, user_id: int):
    """Предложить дальнейшие действия"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать маршруты", callback_data="edit_routes")],
        [InlineKeyboardButton(text="📊 Показать статистику", callback_data="show_stats")],
        [InlineKeyboardButton(text="📤 Экспорт маршрутов", callback_data="export_routes")]
    ])
    
    await message.answer(
        "🎯 *Распределение завершено!*\n\n"
        "Выберите дальнейшее действие:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# --- Редактирование маршрутов ---
@dp.callback_query(F.data == "edit_routes")
async def start_edit_routes(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id not in user_data or 'routes_info' not in user_data[user_id]:
        await callback.answer("Нет данных о маршрутах")
        return
    
    routes_info = user_data[user_id]['routes_info']
    
    # Создаем клавиатуру для выбора маршрута
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🚛 Маршрут {i+1} ({len(info['addresses'])} адр.)",
            callback_data=f"select_source_route_{i}"
        )] for i, info in sorted(routes_info.items())
    ] + [
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")]
    ])
    
    await callback.message.answer(
        "📋 *Редактирование маршрутов*\n\n"
        "Выберите маршрут, из которого хотите переместить адрес:",
        reply_markup=keyboard
    )
    await state.set_state(EditRoutesStates.selecting_route)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_source_route_"))
async def select_source_route(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    route_id = int(callback.data.split("_")[-1])
    
    if user_id not in user_data or route_id not in user_data[user_id]['routes_info']:
        await callback.answer("Маршрут не найден")
        return
    
    info = user_data[user_id]['routes_info'][route_id]
    addresses = info['addresses']
    
    if not addresses:
        await callback.answer("В этом маршруте нет адресов для перемещения")
        return
    
    # Сохраняем выбранный маршрут
    await state.update_data(source_route_id=route_id)
    
    # Создаем клавиатуру с адресами
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"📍 {addr.replace('Москва, ', '')[:25]}...",
            callback_data=f"select_address_{idx}"
        )] for idx, addr in enumerate(addresses)
    ] + [
        [InlineKeyboardButton(text="◀️ Назад к маршрутам", callback_data="back_to_route_select")]
    ])
    
    await callback.message.edit_text(
        f"🚛 *Маршрут {route_id+1}*\n\n"
        f"Выберите адрес для перемещения:",
        reply_markup=keyboard
    )
    await state.set_state(EditRoutesStates.selecting_address)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_address_"))
async def select_address(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    address_idx = int(callback.data.split("_")[-1])
    
    # Сохраняем выбранный адрес
    await state.update_data(address_idx=address_idx)
    
    # Получаем данные о маршрутах
    routes_info = user_data[user_id]['routes_info']
    state_data = await state.get_data()
    source_route_id = state_data.get('source_route_id')
    
    # Создаем клавиатуру с целевыми маршрутами (кроме исходного)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🚛 Маршрут {i+1} ({len(info['addresses'])} адр.)",
            callback_data=f"select_target_route_{i}"
        )] for i, info in sorted(routes_info.items()) if i != source_route_id
    ] + [
        [InlineKeyboardButton(text="◀️ Назад к адресам", callback_data=f"select_source_route_{source_route_id}")]
    ])
    
    # Получаем текст выбранного адреса
    source_route = routes_info[source_route_id]
    selected_address = source_route['addresses'][address_idx]
    short_addr = selected_address.replace("Москва, ", "")[:30]
    
    await callback.message.edit_text(
        f"📍 *Выбран адрес:*\n{short_addr}\n\n"
        f"Выберите маршрут, в который переместить адрес:",
        reply_markup=keyboard
    )
    await state.set_state(EditRoutesStates.selecting_target_route)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_target_route_"))
async def select_target_route(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    target_route_id = int(callback.data.split("_")[-1])
    
    # Получаем сохраненные данные
    state_data = await state.get_data()
    source_route_id = state_data.get('source_route_id')
    address_idx = state_data.get('address_idx')
    
    if None in [source_route_id, address_idx]:
        await callback.answer("Ошибка данных")
        return
    
    # Перемещаем адрес
    source_route = user_data[user_id]['routes_info'][source_route_id]
    target_route = user_data[user_id]['routes_info'][target_route_id]
    
    if address_idx < len(source_route['addresses']):
        address = source_route['addresses'].pop(address_idx)
        target_route['addresses'].append(address)
        
        # Оптимизируем порядок в целевом маршруте
        if target_route['addresses']:
            points = [(addr, user_data[user_id]['address_coords'][addr]) 
                     for addr in target_route['addresses'] if addr in user_data[user_id]['address_coords']]
            production_coords = user_data[user_id]['production_coords']
            
            if points and production_coords:
                optimized_order = optimize_route_nearest_neighbor(production_coords, points)
                target_route['addresses'] = optimized_order
        
        # Пересчитываем маршруты
        await recalculate_routes(user_id)
        
        await callback.answer(f"✅ Адрес перемещен в маршрут {target_route_id+1}")
        
        # Возвращаемся к выбору маршрута
        routes_info = user_data[user_id]['routes_info']
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"🚛 Маршрут {i+1} ({len(info['addresses'])} адр.)",
                callback_data=f"select_source_route_{i}"
            )] for i, info in sorted(routes_info.items())
        ] + [
            [InlineKeyboardButton(text="🏁 Завершить редактирование", callback_data="finish_editing")]
        ])
        
        await callback.message.edit_text(
            "✅ *Адрес успешно перемещен!*\n\n"
            "Выберите маршрут для дальнейшего редактирования:",
            reply_markup=keyboard
        )
        await state.set_state(EditRoutesStates.selecting_route)
    
    else:
        await callback.answer("Ошибка: адрес не найден")

async def recalculate_routes(user_id: int):
    """Пересчитать все маршруты после редактирования"""
    routes_info = user_data[user_id]['routes_info']
    address_coords = user_data[user_id]['address_coords']
    production_coords = user_data[user_id]['production_coords']
    departure_time = user_data[user_id]['departure_time']
    
    for driver_id, info in routes_info.items():
        if info['addresses']:
            points = [(addr, address_coords[addr]) for addr in info['addresses'] if addr in address_coords]
            
            if points and production_coords:
                # Оптимизируем порядок
                optimized_order = optimize_route_nearest_neighbor(production_coords, points)
                info['addresses'] = optimized_order
                
                # Формируем waypoints
                waypoints = [production_coords]
                for addr in optimized_order:
                    if addr in address_coords:
                        waypoints.append(address_coords[addr])
                
                # Рассчитываем маршрут
                route_data = await tomtom_calculate_optimized_route(
                    waypoints, 
                    departure_time,
                    return_to_start=info.get('return_to_base', False)
                )
                
                info['route_data'] = route_data
                info['waypoints'] = waypoints

@dp.callback_query(F.data == "back_to_route_select")
async def back_to_route_select(callback: CallbackQuery, state: FSMContext):
    await start_edit_routes(callback, state)

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Возвращаюсь в главное меню...", reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "finish_editing")
async def finish_editing_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    await callback.message.answer(
        "✅ *Редактирование завершено!*\n"
        "Маршруты пересчитаны с учетом изменений.",
        reply_markup=get_main_keyboard()
    )
    await show_routes(callback.message, user_id)
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "show_stats")
async def show_stats_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    await show_distribution_stats(callback.message, user_id)
    await callback.answer()

@dp.callback_query(F.data == "export_routes")
async def export_routes_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id not in user_data or 'routes_info' not in user_data[user_id]:
        await callback.answer("Нет данных для экспорта")
        return
    
    routes_info = user_data[user_id]['routes_info']
    production_address = PRODUCTION_ADDRESS
    
    export_text = "МАРШРУТЫ ДЛЯ ВОДИТЕЛЕЙ\n"
    export_text += f"Адрес производства: {production_address}\n"
    export_text += f"Время отправления: {user_data[user_id].get('departure_time', 'Не указано')}\n"
    export_text += "=" * 50 + "\n\n"
    
    for driver_id, info in sorted(routes_info.items()):
        addresses = info['addresses']
        
        export_text += f"МАРШРУТ {driver_id+1}\n"
        export_text += f"Количество адресов: {len(addresses)}\n"
        
        if info.get('return_to_base'):
            export_text += "Возврат на базу: ДА\n"
        else:
            export_text += "Возврат на базу: НЕТ\n"
        
        export_text += "-" * 30 + "\n"
        
        for i, addr in enumerate(addresses, 1):
            short_addr = addr.replace("Москва, ", "")
            export_text += f"{i}. {short_addr}\n"
        
        route_data = info.get('route_data', {})
        if route_data:
            summary = route_data.get('routes', [{}])[0].get('summary', {})
            travel_time = summary.get('travelTimeInSeconds', 0) // 60
            distance = summary.get('lengthInMeters', 0) / 1000
            
            if travel_time > 0:
                export_text += f"\nОриентировочное время: {travel_time} мин\n"
            if distance > 0:
                export_text += f"Ориентировочное расстояние: {distance:.1f} км\n"
        
        export_text += "\n" + "=" * 50 + "\n\n"
    
    filename = f"маршруты_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(export_text)
    
    with open(filename, 'rb') as f:
        await callback.message.answer_document(
            types.BufferedInputFile(f.read(), filename=filename),
            caption="📁 Экспортированные маршруты"
        )
    
    os.remove(filename)
    await callback.answer()

@dp.message(F.text == "✏️ Редактировать маршруты")
async def handle_edit_routes(message: types.Message):
    user_id = message.from_user.id
    
    if user_id not in user_data or 'routes_info' not in user_data[user_id]:
        await message.answer("❌ Сначала распределите адреса по маршрутам",
                           reply_markup=get_main_keyboard())
        return
    
    await start_edit_routes(types.CallbackQuery(message=message, data="edit_routes"), None)

@dp.message(F.text == "📊 Статистика")
async def handle_stats(message: types.Message):
    user_id = message.from_user.id
    
    if user_id not in user_data or 'routes_info' not in user_data[user_id]:
        await message.answer("❌ Сначала распределите адреса по маршрутам",
                           reply_markup=get_main_keyboard())
        return
    
    await show_distribution_stats(message, user_id)

@dp.message(F.text == "📤 Экспорт")
async def handle_export(message: types.Message):
    user_id = message.from_user.id
    
    if user_id not in user_data or 'routes_info' not in user_data[user_id]:
        await message.answer("❌ Сначала распределите адреса по маршрутам",
                           reply_markup=get_main_keyboard())
        return
    
    await export_routes_handler(types.CallbackQuery(message=message, data="export_routes"))

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
