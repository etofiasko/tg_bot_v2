import sys
import os
import re
import pandas as pd
from io import BytesIO
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from states import ReportStates, StartNewStates
from bot_db import set_active_db, get_partners, register_user, add_download_history, get_categories, get_subcategories, get_user_role, change_user_role, get_download_history
from config import REPORT_MODULE_PATH

sys.path.insert(0, REPORT_MODULE_PATH)

from document_gen.generator import generate_trade_document # type: ignore

excluded_tnveds_string = (
    "8411,841111,841112,841121,841122,841181,841182,841191,841199,851711,851712,851713,851714,851718,851761,851762,851769,851770,51771,"
    "851779,880211,880212,880220,880230,880240,880260,8411128009,8517610001,8517610002,8411910008,8411123006,8802300002,8802400011,8517,"
    "8411121009,8411123008,8411826001,8411222008,8411110009,8802110002,8411810008,8517693100,8802120001,8411210001,8802200001,8802400036,"
    "8411990019,8411810001,8411822008,8411910002,8802120009,8802300007,8411210009,8411228001,8411123009,8411990011,8802400018,8411910001,"
    "8411110001,8411128002,8411828009,8411990098,8517110000,8517130000,8517140000,8517180000,8517610008,8517620002,8517620003,8517620009,"
    "8517691000,8517692000,8517693900,8517699000,8517711100,8517711500,8517711900,8517790001,8517790009,8802200008,8411121001,8411228008,"
    "8802400039,8802400034,8411822001,8411990091,8411990092,8802300003,8802110009,8517701100,8517709009,8802110003,8802601000,8517120000,"
    "8517701500,8517701900,8517709001,8411222003,8802200002,8802"
)


async def start_handler(message: types.Message, state: FSMContext, user=None):
    set_active_db("main")
    await state.finish()
    user = user or message.from_user
    telegram_id = user.id
    username = user.username or f"user_{telegram_id}"
    register_user(telegram_id, username.strip().lower())
    role = get_user_role(telegram_id)
    if role in ['admin', 'advanced']:
        region = "Республика Казахстан"
        partners = get_partners()
        if not partners:
            await message.reply("Для этого региона нет данных по странам-партнёрам.")
            return

        await state.update_data(region=region)
        await state.update_data(partner_list=partners)

        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        keyboard.add(KeyboardButton("Вернуться назад"))
        for partner in partners:
            keyboard.add(KeyboardButton(partner))

        await message.answer(
            f"Добро пожаловать, {username}.\n\nВыберите страну-партнёра для Республики Казахстан.",
            reply_markup=keyboard
        )
        await ReportStates.choosing_partner.set()
    else:
        await message.reply("У вас нет прав для использования бота.")
        return


async def partner_chosen_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    
    if text.lower() == "начать заново":
        await message.answer("Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await state.finish()
        return
    data = await state.get_data()

    partners = data["partner_list"]
    
    if text not in partners:
        await message.answer("Такого партнёра нет. Пожалуйста, выберите из предложенного списка.")
        return
    
    years = ['2020','2021','2022','2023','2024','2025']
    if not years:
        await message.reply("Для этого региона и страны-партнёра нет данных по годам. Попробуйте выбрать другой регион.")
        await start_handler(message, state)
        return
    
    await state.update_data(partner=text)
    await state.update_data(year_list=years)

    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    for year in years:
        keyboard.add(KeyboardButton(year))
    
    await message.answer("Выберите год:", reply_markup=keyboard)
    await ReportStates.choosing_year.set()


async def year_chosen_handler(message: types.Message, state: FSMContext):
    year = message.text.strip()
    
    if year.lower() == "начать заново":
        await start_handler(message, state)
        return
    
    data = await state.get_data()

    years = data["year_list"]
    if year not in years:
        await message.answer("Такого года нет. Пожалуйста, выберите из предложенного списка.")
        return
    await state.update_data(year=year.strip())
    categories = get_categories()

    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Нет категории"))
    for category in categories:
        keyboard.add(KeyboardButton(category))
    await message.answer(
        "Введите категорию или пропустите данный шаг:",
        reply_markup=keyboard
    )
    
    await ReportStates.choosing_category_settings.set()
        

async def category_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    

    if text.strip().startswith("Нет категории"):
        await state.update_data(category='', subcategory='')
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("Подтвердить выбор", callback_data="confirm"),
            InlineKeyboardButton("Расширенные настройки", callback_data="advanced_settings"),
            InlineKeyboardButton("Отмена", callback_data="cancel")
        )

        await message.answer(
            f"Вы выбрали:\n"
            f"Регион: <b>{(await state.get_data()).get('region')}</b>\n"
            f"Страна-партнёр: <b>{(await state.get_data()).get('partner')}</b>\n"
            f"Год: <b>{(await state.get_data()).get('year')}</b>\n"
            f"Категория: <b>Нет категории</b>\n\n"
            f"Пожалуйста, подтвердите выбор или настройте дополнительные параметры:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        await ReportStates.confirmation.set()
        return
        
    categories = get_categories()
    if text not in categories:
            await message.answer("Такой категории нет. Пожалуйста, выберите из предложенного списка.")
            return
    
    subcats = get_subcategories(text)

    if not subcats:
        await message.answer("В выбранной вами категории нет подкатегорий. Пожалуйста, выберите другую категорию.")
        return

    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    for sc in subcats:
        keyboard.add(KeyboardButton(sc))
    await state.update_data(category=text)
    await message.answer(
        "Введите подкатегорию:",
        reply_markup=keyboard
    )
    await ReportStates.choosing_subcategory_settings.set()


async def subcategory_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    data = await state.get_data()
    category = data.get("category")
    subcategories = get_subcategories(category)
    if text not in subcategories:
            await message.answer("Такой подкатегории нет. Пожалуйста, выберите из предложенного списка.")
            return

    await state.update_data(subcategory=message.text.strip())

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("Подтвердить выбор", callback_data="confirm"),
        InlineKeyboardButton("Расширенные настройки", callback_data="advanced_settings"),
        InlineKeyboardButton("Отмена", callback_data="cancel")
    )

    await message.answer(
        f"Вы выбрали:\n"
        f"Регион: <b>{(await state.get_data()).get('region')}</b>\n"
        f"Страна-партнёр: <b>{(await state.get_data()).get('partner')}</b>\n"
        f"Год: <b>{(await state.get_data()).get('year')}</b>\n"
        f"Категория: <b>{(await state.get_data()).get('category')}</b>\n"
        f"Подкатегория: <b>{(await state.get_data()).get('subcategory')}</b>\n\n"
        f"Пожалуйста, подтвердите выбор или настройте дополнительные параметры:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await ReportStates.confirmation.set()


async def confirmation_handler(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    await callback_query.message.edit_reply_markup(reply_markup=None)

    if callback_query.data == "cancel":
        user_message = callback_query.from_user
        await start_handler(callback_query.message, state, user=user_message)
        return

    if callback_query.data == "confirm":
        await finalize_report(callback_query, state, callback_query.from_user)
        return

    if callback_query.data == "advanced_settings":
        data = await state.get_data()
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        keyboard.add(KeyboardButton("Начать заново"))
        keyboard.add(KeyboardButton("Пропустить"))
        keyboard.add(KeyboardButton("4 знака"))
        keyboard.add(KeyboardButton("6 знаков"))
        if data.get("subcategory") == '':
            keyboard.add(KeyboardButton("10 знаков"))
            await callback_query.message.answer(
                "Введите количество знаков 4, 6, 10 или пропустите данный шаг:",
                reply_markup=keyboard
            )
        else:
            await callback_query.message.answer(
                "Введите количество знаков 4, 6 или пропустите данный шаг:",
                reply_markup=keyboard
            )
        await ReportStates.choosing_digit_settings.set()


async def digit_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    data = await state.get_data()
    if message.text.strip() == "4 знака" or message.text.strip() == "Пропустить":
            message.text = '4'
    elif message.text.strip() == "6 знаков":
            message.text = '6'
    elif message.text.strip() == "10 знаков" and data.get("subcategory") == '':
            message.text = '10'
    else:
        if not text.isdigit():
            if data.get("subcategory") == '':
                await message.answer("Пожалуйста, введите число 4, 6, 10 или пропустите данный шаг.")
                return
            else:
                await message.answer("Пожалуйста, введите число 4, 6 или пропустите данный шаг.")
                return

        value = int(text)
        if data.get("subcategory") == '':
            if value != 4 and value != 6 and value != 10:
                await message.answer("Число должно быть 4, 6 или 10. Попробуйте ещё раз.")
                return
        else:
            if value != 4 and value != 6:
                await message.answer("Число должно быть 4, 6. Попробуйте ещё раз.")
                return

    await state.update_data(digit=message.text.strip())
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Пропустить"))

    await message.answer(
        "Введите нужный месяц в формате X или диапазон месяцев в формате X, Y или пропустите данный шаг:",
        reply_markup=keyboard
    )
    await ReportStates.choosing_months_settings.set()


async def months_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    
    if message.text.strip() == "Пропустить":
        text = ''
    else:
        if "," in text:
            try:
                start, end = map(int, text.split(","))
                if not (1 <= start <= 12 and 1 <= end <= 12):
                    await message.answer("Месяцы должны быть от 1 до 12.")
                    return
                if end < start:
                    await message.answer("Конечный месяц не может быть меньше начального.")
                    return
                if start == end:
                    await message.answer("Начальный и конечный месяц не должны быть одинаковыми.")
                    return
            except ValueError:
                await message.answer("Неверный формат месяцев. Убедитесь, что вы ввели два числа через запятую.")
                return
        else:
            if not text.isdigit():
                await message.answer("Введите нужный месяц в формате X или диапазон месяцев в формате X, Y или пропустите данный шаг:")
                return

            month = int(text)
            if not (1 <= month <= 12):
                await message.answer("Месяц должен быть от 1 до 12.")
                return

    await state.update_data(months=text.strip().replace(" ", ""))
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Пропустить (включить все знаки ТН ВЭД)"))
    keyboard.add(KeyboardButton("Исключить знаки ТН ВЭД по реэкспорту"))
    await message.answer(
        "Введите ТН ВЭД, которые нужно исключить из справки в формате X или несколько ТН ВЭД в формате X, Y, Z или пропустите данный шаг:",
        reply_markup=keyboard
    )
    await ReportStates.choosing_exclude_tnved_settings.set() 


async def exclude_tnved_settings_handler(message: types.Message, state: FSMContext):  
    text = message.text.strip().rstrip(",").lstrip(",")
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    
    if message.text.strip() == "Пропустить (включить все знаки ТН ВЭД)":
        text = ""
    elif message.text.strip() == "Исключить знаки ТН ВЭД по реэкспорту":
        text = excluded_tnveds_string
    else:
        if "," in text:
            try:
                check_tnved = text.replace(',', '').replace(' ', '')
                if not check_tnved.isdigit():
                    await message.answer("Неверный формат ТН ВЭД. Убедитесь, что вы ввели верные данные через запятую.")
                    return
            except ValueError:
                await message.answer("Неверный формат ТН ВЭД. Убедитесь, что вы ввели верные данные через запятую.")
                return
        else:
            if not text.isdigit():
                await message.answer("Неверный формат ТН ВЭД. Убедитесь, что вы ввели верные данные.")
                return
    
    await state.update_data(exclude_tnved=text.replace(" ", ""))
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Пропустить"))
    await message.answer(
        "Введите количество строк товаров от 1 до 500 или пропустите данный шаг:",
        reply_markup=keyboard
    )
    await ReportStates.choosing_table_size_settings.set()


async def table_size_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    
    if message.text.strip() == "Пропустить":
        text = '25'
    else:
        if not text.isdigit():
            await message.answer("Пожалуйста, введите число от 1 до 500 или пропустите данный шаг.")
            return

        value = int(text)
        if value < 1 or value > 500:
            await message.answer("Число должно быть в диапазоне от 1 до 500. Попробуйте ещё раз.")
            return

    await state.update_data(table_size=text)
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Пропустить"))
    await message.answer(
        "Введите количество строк стран от 1 до 250 или пропустите данный шаг:",
        reply_markup=keyboard
    )

    await ReportStates.choosing_country_table_size_settings.set()

async def country_table_size_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    
    if message.text.strip() == "Пропустить":
        text = '15'
    else:
        if not text.isdigit():
            await message.answer("Пожалуйста, введите число от 1 до 250 или пропустите данный шаг.")
            return

        value = int(text)
        if value < 1 or value > 250:
            await message.answer("Число должно быть в диапазоне от 1 до 250. Попробуйте ещё раз.")
            return

    await state.update_data(country_table_size=text)
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Начать заново"))
    keyboard.add(KeyboardButton("Пропустить"))
    await message.answer(
        "Введите количество текста товаров от 1 до 20 или пропустите данный шаг:",
        reply_markup=keyboard
    )
    await ReportStates.choosing_text_size_settings.set()

async def text_size_settings_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if message.text.lower().strip() == "начать заново":
        await start_handler(message, state)
        return
    
    if message.text.strip() == "Пропустить":
        text = '7'
    else:
        if not text.isdigit():
            await message.answer("Пожалуйста, введите число от 1 до 20 или пропустите данный шаг.")
            return

        value = int(text)
        if value < 1 or value > 20:
            await message.answer("Число должно быть в диапазоне от 1 до 20. Попробуйте ещё раз.")
            return

    await state.update_data(text_size=text)
    await finalize_report(message, state, message.from_user)


async def finalize_report(msg_or_cbq, state, tg_user):
    telegram_id = tg_user.id
    data = await state.get_data()
    region = str(data["region"])
    partner = str(data["partner"])
    year = int(data["year"])
    digit = int(data.get("digit") or 4)
    subcategory = (data.get("subcategory") or None)
    if data.get("exclude_tnved") != "" and not data.get("exclude_tnved"):
        exclude_tnved = excluded_tnveds_string
    else:
        exclude_tnved = str(data.get("exclude_tnved"))
    months = str(data.get("months") or "")
    table_size = int(data.get("table_size") or 25)
    text_size = int(data.get("text_size") or 7)
    country_table_size = int(data.get("country_table_size") or 15)
    if isinstance(msg_or_cbq, types.CallbackQuery):
        await msg_or_cbq.message.answer(f"❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup = ReplyKeyboardRemove())
    else:
        await msg_or_cbq.answer(f"❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup = ReplyKeyboardRemove())
    try:
        doc, filename, short_filename = generate_trade_document(
            region=region,
            country_or_group=partner,
            year=year,
            digit=digit,
            category=subcategory,
            text_size=text_size,
            table_size=table_size,
            country_table_size=country_table_size,
            month_range_raw=months,
            exclude_raw=exclude_tnved,
            tn_ved_raw=None,
            plain=0,
        )

    except Exception as e:
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer(f"Произошла ошибка при генерации файла. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
            await state.finish()
        else:
            await msg_or_cbq.answer(f"Произошла ошибка при генерации файла. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
            await state.finish()
        return
    
    if filename != 'Данных нет':
        buf = BytesIO()
        doc.save(buf)
        buf.seek(0)

        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer_document((short_filename, buf))
            await msg_or_cbq.message.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        else:
            await msg_or_cbq.answer_document((short_filename, buf))
            await msg_or_cbq.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await add_download_history(telegram_id, region, partner, year)
        await state.finish()
    else:
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer(f"По выбранным фильтрам нет данных. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        else:
            await msg_or_cbq.answer(f"По выбранным фильтрам нет данных. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await state.finish()


async def access_settings_handler(message: types.Message):
    role = get_user_role(message.from_user.id)
    if role != 'admin':
        await message.answer("У вас нет прав для управления доступами.")
        return
    await ReportStates.waiting_for_access_data.set()
    await message.answer(f"Введите данные в формате: \n@username роль \n<b>advanced</b> - доступ к боту \n<b>user</b> - нет доступа", parse_mode='html')

async def handle_access_data(message: types.Message, state: FSMContext):
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Некорректный формат. Повторите снова. Укажите @username и роль \n<b>advanced</b> - доступ к боту \n<b>user</b> - нет доступа", parse_mode='html')
        await state.finish()
        return

    username, new_role = args[0].strip().strip('@').lower(), args[1].strip().lower()
    if new_role not in ['admin', 'advanced', 'user']:
        await message.reply("Некорректная роль. Повторите снова. Доступные роли: \n<b>advanced</b> - доступ к боту \n<b>user</b> - нет доступа", parse_mode='html')
        await state.finish()
        return

    await state.finish()
    change_user_role_reply = await change_user_role(username, new_role)
    await message.answer(f'{change_user_role_reply}')


async def download_history_handler(message: types.Message):
    role = get_user_role(message.from_user.id)
    if role != 'admin':
        await message.answer("У вас нет прав для просмотра истории.")
        return

    get_download_history_reply, get_download_history_rows = await get_download_history()

    if get_download_history_reply:
        await message.answer(f'{get_download_history_reply}')
    df = pd.DataFrame(get_download_history_rows, columns=["ID", "Username", "Region", "Partner", "Year", "Downloaded At"])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    await message.answer_document(("download_history.xlsx", output))
















async def start_new_handler(message: types.Message, state: FSMContext, user=None):
    await state.finish()
    set_active_db("alt")
    user = user or message.from_user
    telegram_id = user.id
    username = user.username or f"user_{telegram_id}"
    register_user(telegram_id, username.strip().lower())
    role = get_user_role(telegram_id)
    if role not in ['admin', 'advanced']:
        await message.reply("У вас нет прав для использования бота.")
        return

    await state.update_data(region="Республика Казахстан")

    await state.update_data(
        digit=4,
        months="",
        exclude_tnved="",
        table_size=25,
        text_size=7,
        country_table_size=15,
        subcategory=None,
        plain=0,
        tn_ved=""
    )

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("Самолётик", callback_data="plane_cb"),
        InlineKeyboardButton("По стране", callback_data="country_cb"),
    )
    kb.add(InlineKeyboardButton("По товару", callback_data="product_cb"),
        InlineKeyboardButton("Вернуться назад", callback_data="cancel_cb"))

    await message.answer(f"Добро пожаловать, {username}. \n\nВыберите тип справки для генерации:", reply_markup=kb)
    await StartNewStates.choosing_variant.set()


async def start_new_variant_chosen(cbq: CallbackQuery, state: FSMContext):
    await cbq.answer()
    data = cbq.data
    await cbq.message.edit_reply_markup(reply_markup=None)

    partners = get_partners()

    partners_kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    partners_kb.add(KeyboardButton("Начать заново"))
    for p in partners:
        partners_kb.add(KeyboardButton(p))
    

    if data == "cancel_cb":
        await cbq.message.answer("Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await state.finish()
        return

    if data == "plane_cb":
        await state.update_data(plain=1, tn_ved="", subcategory=None)
        await cbq.message.answer("Выберите страну-партнёра для Республики Казахстан.", reply_markup=partners_kb)
        await StartNewStates.choosing_partner.set()

    if data == "country_cb":
        await state.update_data(plain=0, tn_ved="", subcategory=None)
        await cbq.message.answer("Выберите страну-партнёра для Республики Казахстан.", reply_markup=partners_kb)
        await StartNewStates.choosing_partner.set()

    if data == "product_cb":
        await state.update_data(plain=0, tn_ved="", subcategory=None)
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        keyboard.add(KeyboardButton("Начать заново"))
        await cbq.message.answer("Введите код ТН ВЭД, только цифры (от 2 до 10 знаков).", reply_markup=keyboard)
        await StartNewStates.waiting_for_tnved.set()


async def start_new_waiting_tnved(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    if not re.fullmatch(r"\d{2,10}", txt):
        await message.answer("Неверный формат ТН ВЭД. Убедитесь, что вы ввели только цифры (от 2 до 10 знаков).")
        return

    await state.update_data(tn_ved=txt)

    partners = get_partners()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    for p in partners:
        kb.add(KeyboardButton(p))
    await message.answer("Выберите страну-партнёра для Республики Казахстан.", reply_markup=kb)
    await StartNewStates.choosing_partner.set()


async def start_new_partner(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    partners = get_partners()
    if txt not in partners:
        await message.answer("Такого партнёра нет. Пожалуйста, выберите из предложенного списка.")
        return
    await state.update_data(partner=txt)

    years = ['2020','2021','2022','2023','2024','2025']
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    for y in years:
        kb.add(KeyboardButton(str(y)))
    await message.answer("Выберите год:", reply_markup=kb)
    await StartNewStates.choosing_year.set()


async def start_new_year(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    years = ['2020','2021','2022','2023','2024','2025']
    if txt not in years:
        await message.answer("Такого года нет. Пожалуйста, выберите из предложенного списка.")
        return
    await state.update_data(year=txt)

    data = await state.get_data()
    tn_ved = data.get("tn_ved", "")
    plain = int(data.get("plain") or 0)

    if plain == 1 or tn_ved:
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("Подтвердить", callback_data="sn_confirm"),
            InlineKeyboardButton("Отмена", callback_data="sn_restart"),
        )

        summary = []
        if plain == 1:
            summary.append(f"Вид справки: <b>Самолётик</b>")
        if tn_ved:
            summary.append(f"Вид справки: <b>По товару</b>")
            summary.append(f"ТН ВЭД: <b>{tn_ved}</b>")
        if not tn_ved and plain != 1:
            summary.append(f"Вид справки: <b>По стране</b>")
        summary.append(f"Страна-партнёр: <b>{data.get('partner')}</b>")
        summary.append(f"Год: <b>{data.get('year')}</b>")
        
        await message.answer("\n".join(summary), parse_mode="HTML", reply_markup=kb)
        await StartNewStates.confirmation.set()
        return

    categories = get_categories()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    kb.add(KeyboardButton("Нет категории"))
    for c in categories:
        kb.add(KeyboardButton(c))
    await message.answer("Введите категорию или пропустите данный шаг:", reply_markup=kb)
    await StartNewStates.choosing_category.set()


async def start_new_category(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    if txt.startswith("Нет категории"):
        await state.update_data(subcategory="")
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("Подтвердить", callback_data="sn_confirm"),
            InlineKeyboardButton("Отмена", callback_data="sn_restart"),
        )
        d = await state.get_data()
        await message.answer(
            f"Вид справки: <b>По стране</b>\n"
            f"Страна-партнёр: <b>{d.get('partner')}</b>\n"
            f"Год: <b>{d.get('year')}</b>\n"
            f"Категория: <b>Нет категории</b>",
            parse_mode="HTML", reply_markup=kb
        )
        await StartNewStates.confirmation.set()
        return

    categories = get_categories()
    if txt not in categories:
        await message.answer("Такой категории нет. Пожалуйста, выберите из предложенного списка.")
        return

    await state.update_data(category_parent=txt)
    subcats = get_subcategories(txt)
    if not subcats:
        await message.answer("В выбранной вами категории нет подкатегорий. Пожалуйста, выберите другую категорию.")
        return

    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    for sc in subcats:
        kb.add(KeyboardButton(sc))
    await message.answer("Выберите подкатегорию:", reply_markup=kb)
    await StartNewStates.choosing_subcategory.set()


async def start_new_subcategory(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    d = await state.get_data()
    subcats = get_subcategories(d.get("category_parent"))
    if txt not in subcats:
        await message.answer("Такой подкатегории нет. Пожалуйста, выберите из предложенного списка.")
        return

    await state.update_data(subcategory=txt)

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("Подтвердить", callback_data="sn_confirm"),
        InlineKeyboardButton("Отмена", callback_data="sn_restart"),
    )
    d = await state.get_data()
    await message.answer(
        f"Вид справки: <b>По стране</b>\n"
        f"Страна-партнёр: <b>{d.get('partner')}</b>\n"
        f"Год: <b>{d.get('year')}</b>\n"
        f"Категория: <b>{d.get('category_parent')}</b>\n"
        f"Подкатегория: <b>{d.get('subcategory')}</b>",
        parse_mode="HTML", reply_markup=kb
    )
    await StartNewStates.confirmation.set()


async def start_new_confirmation(cbq: CallbackQuery, state: FSMContext):
    await cbq.answer()
    await cbq.message.edit_reply_markup(reply_markup=None)
    if cbq.data == "sn_restart":
        user_message = cbq.from_user
        await start_new_handler(cbq.message, state, user=user_message)
        return
    if cbq.data == "sn_confirm":
        await finalize_report_start_new(cbq, state, cbq.from_user)


async def finalize_report_start_new(msg_or_cbq, state, tg_user):
    telegram_id = tg_user.id
    d = await state.get_data()

    region = "Республика Казахстан"
    partner = str(d["partner"])
    year = int(d["year"])

    digit = int(d.get("digit") or 4)
    months = str(d.get("months") or "")
    exclude_tnved = str(d.get("exclude_tnved") or "")
    table_size = int(d.get("table_size") or 25)
    text_size = int(d.get("text_size") or 7)
    country_table_size = int(d.get("country_table_size") or 15)

    tn_ved = (d.get("tn_ved") or "").strip()
    subcategory = (d.get("subcategory") or None)
    if tn_ved:
        subcategory = None

    if isinstance(msg_or_cbq, types.CallbackQuery):
        await msg_or_cbq.message.answer("❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup=ReplyKeyboardRemove())
    else:
        await msg_or_cbq.answer("❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup=ReplyKeyboardRemove())

    try:
        print(region,partner,year,digit,subcategory,text_size,table_size,country_table_size,months,
              months,exclude_tnved,tn_ved,int(d.get("plain") or 0))
        
        doc, filename, short_filename = generate_trade_document(
            region=region,
            country_or_group=partner,
            year=year,
            digit=digit,
            category=subcategory,
            text_size=text_size,
            table_size=table_size,
            country_table_size=country_table_size,
            month_range_raw=months,
            exclude_raw=exclude_tnved,
            tn_ved_raw=tn_ved,
            plain=int(d.get("plain") or 0),
        )
        
    except Exception:
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer("Произошла ошибка при генерации файла. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
            await state.finish()
        else:
            await msg_or_cbq.answer("Произошла ошибка при генерации файла. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
            await state.finish()
        return

    if filename != 'Данных нет':
        buf = BytesIO(); doc.save(buf); buf.seek(0)
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer_document((short_filename, buf))
            await msg_or_cbq.message.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        else:
            await msg_or_cbq.answer_document((short_filename, buf))
            await msg_or_cbq.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await add_download_history(telegram_id, region, partner, year)
        await state.finish()
    else:
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer("По выбранным фильтрам нет данных. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        else:
            await msg_or_cbq.answer("По выбранным фильтрам нет данных. Чтобы начать заново, нажмите \n/start для tg_bot_v1\n/start_new для tg_bot_v2")
        await state.finish()
