import os
import re
import asyncio
import pdfplumber
import pandas as pd
import numpy as np
import uuid
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from geopy.geocoders import Nominatim
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
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

# --- Логика Геокодинга и Маршрутов ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v17_stable")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: 
        return None

def balanced_kmeans_clustering(df, n_clusters):
    """
    Балансированная кластеризация с примерным равенством точек в кластерах
    """
    if n_clusters <= 1 or len(df) <= n_clusters:
        return KMeans(n_clusters=n_clusters, n_init=10).fit(df[['lat', 'lon']]).labels_
    
    # Стандартная KMeans кластеризация
    kmeans = KMeans(n_clusters=n_clusters, n_init=10)
    labels = kmeans.fit_predict(df[['lat', 'lon']])
    
    # Балансировка кластеров
    cluster_counts = pd.Series(labels).value_counts()
    max_count = cluster_counts.max()
    min_count = cluster_counts.min()
    
    # Если разница между самым большим и самым маленьким кластером > 2, балансируем
    if max_count - min_count > 2:
        centroids = kmeans.cluster_centers_
        
        # Находим точки, которые можно переместить
        for _ in range(10):  # Ограничим количество итераций
            cluster_counts = pd.Series(labels).value_counts()
            max_cluster = cluster_counts.idxmax()
            min_cluster = cluster_counts.idxmin()
            
            if cluster_counts[max_cluster] - cluster_counts[min_cluster] <= 2:
                break
            
            # Находим точку в самом большом кластере, ближайшую к центроиду самого маленького
            max_cluster_points = df[labels == max_cluster]
            min_centroid = centroids[min_cluster]
            
            # Вычисляем расстояния от точек большого кластера до центроида маленького
            distances = cdist(max_cluster_points[['lat', 'lon']], [min_centroid])
            
            # Находим индекс ближайшей точки
            nearest_idx = distances.argmin()
            
            # Перемещаем точку
            point_idx = max_cluster_points.iloc[[nearest_idx]].index[0]
            labels[point_idx] = min_cluster
    
    return labels

def build_optimal_route(points_coords):
    """
    Строит оптимальный маршрут для заданных точек методом ближайшего соседа
    """
    if len(points_coords) <= 1:
        return list(range(len(points_coords)))
    
    n_points = len(points_coords)
    visited = [False] * n_points
    route = []
    
    # Начинаем с первой точки (условно считаем ее стартовой)
    current = 0
    route.append(current)
    visited[current] = True
    
    for _ in range(n_points - 1):
        # Находим ближайшую непосещенную точку
        min_dist = float('inf')
        nearest_idx = -1
        
        for i in range(n_points):
            if not visited[i]:
                # Вычисляем евклидово расстояние
                dist = np.sqrt(
                    (points_coords[i][0] - points_coords[current][0])**2 +
                    (points_coords[i][1] - points_coords[current][1])**2
                )
                
                if dist < min_dist:
                    min_dist = dist
                    nearest_idx = i
        
        if nearest_idx != -1:
            current = nearest_idx
            route.append(current)
            visited[current] = True
    
    return route

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Бот V17 готов. Загрузка файлов оптимизирована (без ошибок флуда).")

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
            
            # Удаляем сообщение об обработке
            await processing_msg.delete()

            if addr:
                if message.from_user.id not in user_data: 
                    user_data[message.from_user.id] = {'addresses': []}
                user_data[message.from_user.id]['addresses'].append(addr)
                
                # Считаем количество адресов
                total_addresses = len(user_data[message.from_user.id]['addresses'])
                
                await message.answer(
                    f"✅ **Адрес добавлен:**\n`{addr}`\n\n"
                    f"📊 Всего адресов: {total_addresses}",
                    parse_mode="Markdown"
                )
                
                # Автоматически запрашиваем количество водителей
                await ask_drivers_auto(message)
            else:
                await message.answer(f"❌ Ошибка распознавания в {message.document.file_name}")
    except Exception as e:
        # Удаляем сообщение об обработке даже в случае ошибки
        try:
            await processing_msg.delete()
        except:
            pass
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)}")
    finally:
        if os.path.exists(temp_fn): 
            os.remove(temp_fn)

async def ask_drivers_auto(message: types.Message):
    """Автоматический запрос количества водителей"""
    u_id = message.from_user.id
    
    if u_id not in user_data or not user_data[u_id]['addresses']:
        return
    
    # Даем небольшую паузу перед запросом
    await asyncio.sleep(0.5)
    
    total_addresses = len(user_data[u_id]['addresses'])
    # Определяем максимальное количество водителей (не более количества адресов и не более 6)
    max_drivers = min(total_addresses, 6)
    
    # Создаем клавиатуру в зависимости от количества адресов
    kb = []
    if max_drivers >= 1:
        kb.append([KeyboardButton(text=str(i)) for i in range(1, min(4, max_drivers + 1))])
    if max_drivers >= 4:
        kb.append([KeyboardButton(text=str(i)) for i in range(4, max_drivers + 1)])
    
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        f"📦 *Обработка завершена!*\n"
        f"📊 Всего адресов: {total_addresses}\n\n"
        f"🚚 *На скольких водителей распределить адреса?*\n"
        f"(Выберите от 1 до {max_drivers})",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers_manual(message: types.Message):
    """Ручной запрос количества водителей (по кнопке)"""
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("❌ Сначала пришлите PDF-файлы с накладными!")
        return
    
    total_addresses = len(user_data[u_id]['addresses'])
    # Определяем максимальное количество водителей (не более количества адресов и не более 6)
    max_drivers = min(total_addresses, 6)
    
    # Создаем клавиатуру в зависимости от количества адресов
    kb = []
    if max_drivers >= 1:
        kb.append([KeyboardButton(text=str(i)) for i in range(1, min(4, max_drivers + 1))])
    if max_drivers >= 4:
        kb.append([KeyboardButton(text=str(i)) for i in range(4, max_drivers + 1)])
    
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    
    await message.answer(
        f"📊 Всего адресов: {total_addresses}\n"
        f"🚚 *На скольких водителей распределить адреса?*\n"
        f"(Выберите от 1 до {max_drivers})",
        reply_markup=markup,
        parse_mode="Markdown"
    )

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    
    if user_id not in user_data or not user_data[user_id]['addresses']:
        await message.answer("❌ Нет адресов для обработки!")
        return
    
    total_addresses = len(user_data[user_id]['addresses'])
    if num_drivers > total_addresses:
        await message.answer(f"❌ Количество водителей ({num_drivers}) не может быть больше количества адресов ({total_addresses})!")
        return
    
    if num_drivers > 6:
        await message.answer("❌ Максимальное количество водителей - 6!")
        return
    
    raw_addresses = list(set(user_data[user_id]['addresses']))
    
    # Показываем индикатор обработки маршрутов
    progress_msg = await message.answer("🗺️ *Строю оптимальные маршруты...*", parse_mode="Markdown")
    
    # Показываем индикатор поиска местоположения
    await bot.send_chat_action(message.chat.id, "find_location")

    # Собираем координаты
    data = []
    for addr in raw_addresses:
        coords = get_coords(addr)
        if not coords: 
            # Пробуем геокодировать только название улицы
            street_part = ', '.join(addr.split(',')[:2])
            coords = get_coords(street_part)
        if coords: 
            data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        else:
            # Если не нашли координаты, пропускаем этот адрес
            continue
        
        # Пауза для геокодера
        await asyncio.sleep(1.1)

    if not data:
        await progress_msg.edit_text("❌ Ошибка поиска координат на карте.")
        return

    df = pd.DataFrame(data)
    
    # Используем балансированную кластеризацию
    n_cl = min(num_drivers, len(df))
    labels = balanced_kmeans_clustering(df, n_cl)
    df['driver'] = labels

    # Обновляем сообщение о прогрессе
    await progress_msg.edit_text("✅ *Маршруты построены!*\n📋 *Распределение по водителям:*", parse_mode="Markdown")

    # Отправляем маршруты
    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        
        if len(driver_points) == 0:
            continue
            
        # Строим оптимальный маршрут для этого водителя
        points_coords = list(zip(driver_points['lat'], driver_points['lon']))
        route_order = build_optimal_route(points_coords)
        
        res = f"🚛 *МАРШРУТ №{i+1}* ({len(driver_points)} адрес(ов))\n\n"
        
        # Формируем адреса в правильном порядке
        ordered_addresses = driver_points.iloc[route_order]['address'].tolist()
        
        for j, address in enumerate(ordered_addresses, 1):
            # Убираем "Москва, " для более компактного отображения
            final_view = address.replace("Москва, ", "")
            res += f"{j}. {final_view}\n"
        
        await message.answer(res, parse_mode="Markdown")
    
    # Показываем статистику
    stats = f"📊 *Статистика распределения:*\n"
    stats += f"• Всего обработано адресов: {len(data)}\n"
    stats += f"• Распределено на водителей: {n_cl}\n\n"
    
    for i in range(n_cl):
        driver_count = len(df[df['driver'] == i])
        stats += f"• Водитель {i+1}: {driver_count} адрес(ов)\n"
    
    await message.answer(stats, parse_mode="Markdown")
    
    # Очищаем данные пользователя
    user_data[user_id] = {'addresses': []}
    
    # Удаляем сообщение о прогрессе
    await progress_msg.delete()

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
