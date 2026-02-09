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

# --- СИСТЕМА ОЧИСТКИ V13 (БЕЗОПАСНАЯ) ---
def clean_address(text):
    # 1. Ищем блок КЛИЕНТА (Грузополучателя)
    # Игнорируем всё, что выше или ниже этого конкретного блока
    raw = ""
    # Пытаемся найти зону между Грузополучателем и Поставщиком
    target_block = re.search(r"Грузополучатель(.*?)(Поставщик|Основание|Пункт|Транспортная)", text, re.DOTALL | re.IGNORECASE)
    if target_block:
        raw = target_block.group(1).replace('\n', ' ')
    else:
        # Резервный поиск, если структура чуть съехала
        target_block = re.search(r"Грузополучатель(.*?)\d{2}\.\d{2}\.\d{4}", text, re.DOTALL | re.IGNORECASE)
        if target_block:
            raw = target_block.group(1).replace('\n', ' ')

    if not raw: return None

    # 2. УДАЛЯЕМ ВСЕ ДЛИННЫЕ ЧИСЛА (Счета, ИНН, КПП, ОКПО)
    # Любая цепочка из 7 и более цифр — это не дом и не индекс (индексы 6 цифр, мы их тоже уберем)
    raw = re.sub(r'\d{7,}', '', raw)
    raw = re.sub(r'\b\d{6}\b', '', raw)

    # 3. УДАЛЯЕМ МУСОРНЫЕ СЛОВА (Юр. лица и банковские термины)
    trash_words = [
        r'Общество\s+с\s+ограниченной\s+ответственностью', r'ООО', r'ИП', r'АО', r'ПАО',
        r'ХЭДВЭЙ\s+ИНВЕСТ', r'реквизиты', r'телефон', r'факс', r'ОКПО', r'ИНН', r'КПП',
        r'БИК', r'Банк', r'филиал', r'комн\.?\s*\d+', r'пом\.?\s*\d+', r'адрес'
    ]
    for word in trash_words:
        raw = re.sub(word, '', raw, flags=re.IGNORECASE)

    # 4. ВЫДЕЛЯЕМ ГЕО-ОБЪЕКТЫ (Улица и Дом)
    # Ищем: ул, проспект, пр-т, шоссе, наб, пер, бульвар + номер дома
    parts = raw.split(',')
    clean_parts = []
    
    # Ключевые паттерны адреса
    geo_markers = r'(ул\.|ул\s|пр-т|проспект|наб|пер\.|бульвар|шоссе|площадь|д\.|дом|к\.|корп\.)'
    
    for p in parts:
        p_clean = p.strip()
        # Если в куске текста есть маркер улицы или это похоже на номер дома
        if re.search(geo_markers, p_clean, re.IGNORECASE) or re.search(r'\d+[а-яА-Я]?$', p_clean):
            # Доп. чистка от кавычек и лишних слов
            p_clean = re.sub(r'[«»"]', '', p_clean)
            p_clean = re.sub(r'\b(г\.|г|город)\b', '', p_clean, flags=re.IGNORECASE).strip()
            
            if len(p_clean) > 1 and "Москва" not in p_clean:
                clean_parts.append(p_clean)

    # Собираем итоговую строку
    if not clean_parts: return None
    
    # Всегда начинаем с Москвы
    result = "Москва, " + ", ".join(clean_parts)

    # 5. ФИНАЛЬНОЕ ПРИЧЕСЫВАНИЕ
    result = re.sub(r'\bул\b(?!\.)', 'ул.', result, flags=re.IGNORECASE)
    # Склейка корпусов: 23 к 1 -> 23к1
    result = re.sub(r'(\d+)\s*[, ]\s*(?:корп\.?|к\.)\s*(\d+)', r'\1к\2', result, flags=re.IGNORECASE)
    # Склейка литеры: 13 А -> 13А
    result = re.sub(r'(\d+)\s+([А-Яа-я])\b', r'\1\2', result)
    # Ставим запятую перед домом (Улица 5 -> Улица, 5)
    result = re.sub(r'([а-яА-ЯёЁ]{4,})\s+(\d+)', r'\1, \2', result)
    # Удаляем вн.тер. и округа
    result = re.sub(r'вн\.?тер\.?[^,]*', '', result, flags=re.IGNORECASE)
    
    # Чистка двойных запятых
    result = re.sub(r'[,]{2,}', ',', result)
    result = re.sub(r'\s+', ' ', result)
    
    return result.strip(' ,.')

# --- ГЕОКОДИНГ И ЛОГИКА БОТА ---
def get_coords(address):
    try:
        geolocator = Nominatim(user_agent="logistic_v13_final")
        location = geolocator.geocode(address, timeout=10)
        return (location.latitude, location.longitude) if location else None
    except: return None

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'addresses': []}
    await message.answer("Версия V13. Я исправил ошибку с реквизитами вашей компании. Присылайте PDF.")

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if not message.document.file_name.lower().endswith('.pdf'): return
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
                await message.answer(f"❌ Не удалось найти адрес в {message.document.file_name}")
    finally:
        if os.path.exists(temp_fn): os.remove(temp_fn)

@dp.message(F.text == "🚚 Начать обработку накладных")
async def ask_drivers(message: types.Message):
    u_id = message.from_user.id
    if u_id not in user_data or not user_data[u_id]['addresses']:
        await message.answer("Пришлите сначала накладные!"); return
    kb = [[KeyboardButton(text=str(i)) for i in range(1, 4)], [KeyboardButton(text=str(i)) for i in range(4, 7)]]
    markup = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(f"Найдено: {len(user_data[u_id]['addresses'])}. Сколько водителей?", reply_markup=markup)

@dp.message(F.text.regexp(r'^\d+$'))
async def process_logistics(message: types.Message):
    num_drivers = int(message.text)
    user_id = message.from_user.id
    raw_addresses = list(set(user_data[user_id]['addresses']))
    status = await message.answer("⏳ **Ищу адреса на карте...**")
    data = []
    for addr in raw_addresses:
        await bot.send_chat_action(message.chat.id, "find_location")
        coords = get_coords(addr)
        if not coords: coords = get_coords(", ".join(addr.split(',')[:2]))
        if coords: data.append({'address': addr, 'lat': coords[0], 'lon': coords[1]})
        await asyncio.sleep(1.2)
    if not data:
        await status.edit_text("❌ Ошибка поиска на карте."); return
    df = pd.DataFrame(data)
    n_cl = min(num_drivers, len(df))
    kmeans = KMeans(n_clusters=n_cl, n_init=10).fit(df[['lat', 'lon']])
    df['driver'] = kmeans.labels_
    await status.delete()
    for i in range(n_cl):
        driver_points = df[df['driver'] == i]
        res = f"🚛 **МАРШРУТ ВОДИТЕЛЯ №{i+1}**\n"
        for _, row in driver_points.iterrows():
            final_view = row['address'].replace("Москва, ", "")
            res += f"📍 {final_view}\n"
        await message.answer(res, parse_mode="Markdown")
    user_data[user_id] = {'addresses': []}

async def main():
    await asyncio.gather(start_web_server(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
