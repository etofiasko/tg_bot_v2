import sys
import os
import re
import pandas as pd
from io import BytesIO
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from states import StartNewStates
from bot_db import tnved_exists, get_partners, register_user, add_download_history, get_categories, get_subcategories, get_user_role, change_user_role, get_download_history, get_users_for_export
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

async def access_settings_handler(message: types.Message):
    role = get_user_role(message.from_user.id)
    if role != 'admin':
        await message.answer("У вас нет прав для управления доступами.")
        return
    
    rows = await get_users_for_export()
    if rows:
        df = pd.DataFrame(rows, columns=["ID", "Telegram ID", "Username", "Role"])
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        await message.answer_document(("users.xlsx", output))
    else:
        await message.answer("Пока в базе нет пользователей.")

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("Отмена"))

    await StartNewStates.waiting_for_access_data.set()
    await message.answer(
    "Введите данные в формате:\n"
    "<code>telegram_id роль</code>\n\n"
    "<b>advanced</b> – доступ к боту\n"
    "<b>user</b> – нет доступа\n\n"
    "Пример: <code>123456789 advanced</code>",
    parse_mode='html', reply_markup=kb
    )

async def handle_access_data(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "отмена":
        await state.finish()
        await message.answer("Вы вышли из настроек доступа.", reply_markup=ReplyKeyboardRemove())
        return
    
    args = message.text.split()
    if len(args) != 2:
        await message.reply(
            "Некорректный формат.\n"
            "Укажите <code>telegram_id</code> и роль через пробел.\n"
            "<b>advanced</b> – доступ к боту\n"
            "<b>user</b> – нет доступа\n\n"
            "Пример: <code>123456789 advanced</code>",
            parse_mode='html'
        )
        await state.finish()
        return

    tg_id_raw, new_role = args[0].strip(), args[1].strip().lower()

    if not tg_id_raw.isdigit():
        await message.reply(
            "telegram_id должен быть числом.\n"
            "Пример: <code>123456789 advanced</code>",
            parse_mode='html'
        )
        await state.finish()
        return

    telegram_id = int(tg_id_raw)

    if new_role not in ['admin', 'advanced', 'user']:
        await message.reply(
            "Некорректная роль.\n"
            "Доступные роли:\n"
            "<b>advanced</b> – доступ к боту\n"
            "<b>user</b> – нет доступа",
            parse_mode='html'
        )
        await state.finish()
        return

    await state.finish()
    change_user_role_reply = await change_user_role(telegram_id, new_role)
    await message.answer(change_user_role_reply)



async def download_history_handler(message: types.Message):
    role = get_user_role(message.from_user.id)
    if role != 'admin':
        await message.answer("У вас нет прав для просмотра истории.")
        return

    get_download_history_reply, get_download_history_rows = await get_download_history()

    if get_download_history_reply:
        await message.answer(f'{get_download_history_reply}')
    df = pd.DataFrame(get_download_history_rows, columns=["ID", "Username", "Filter", "Year", "Downloaded At"])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    await message.answer_document(("download_history.xlsx", output))



async def start_new_handler(message: types.Message, state: FSMContext, user=None):
    await state.finish()
    user = user or message.from_user
    telegram_id = user.id
    username = user.username or f"user_{telegram_id}"
    register_user(telegram_id, username.strip())
    role = get_user_role(telegram_id)
    if role not in ['admin', 'advanced']:
        await message.reply("У вас нет прав для использования бота.")
        return

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
        await cbq.message.answer("Чтобы начать заново, нажмите /start")
        await state.finish()
        return

    if data == "plane_cb":
        await state.update_data(plain=1, tn_ved="", subcategory=None)
        await cbq.message.answer("Выберите страну-партнёра для Республики Казахстан:", reply_markup=partners_kb)
        await StartNewStates.choosing_partner.set()

    if data == "country_cb":
        await state.update_data(plain=0, tn_ved="", subcategory=None)
        await cbq.message.answer("Выберите страну-партнёра для Республики Казахстан:", reply_markup=partners_kb)
        await StartNewStates.choosing_partner.set()

    if data == "product_cb":
        await state.update_data(plain=0, tn_ved="", subcategory=None)
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        keyboard.add(KeyboardButton("Начать заново"))
        await cbq.message.answer("Введите код ТН ВЭД. Код ТН ВЭД должен состоять только из цифр и быть длиной 4, 6 или 10 знаков.", reply_markup=keyboard)
        await StartNewStates.waiting_for_tnved.set()


async def start_new_waiting_tnved(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    if not re.fullmatch(r"(?:\d{4}|\d{6}|\d{10})", txt):
        await message.answer("Неверный формат ТН ВЭД. Код ТН ВЭД должен состоять только из цифр и быть длиной 4, 6 или 10 знаков.")
        return
    
    if not tnved_exists(txt):
        await message.answer("Такого кода ТН ВЭД нет в базе. Проверьте правильность ввода.")
        return

    await state.update_data(tn_ved=txt, digit=len(txt), partner='весь мир')

    years = ['2020','2021','2022','2023','2024','2025']
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    for y in years:
        kb.add(KeyboardButton(str(y)))
    await message.answer("Выберите год:", reply_markup=kb)
    await StartNewStates.choosing_year.set()


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
        summary.append(f"Вы выбрали:")
        if plain == 1:
            summary.append(f"Вид справки: <b>Самолётик</b>")
        if tn_ved:
            summary.append(f"Вид справки: <b>По товару</b>")
            summary.append(f"ТН ВЭД: <b>{tn_ved}</b>")
        if not tn_ved and plain != 1:
            summary.append(f"Вид справки: <b>По стране</b>")
        summary.append(f"Страна-партнёр: <b>{data.get('partner')}</b>")
        summary.append(f"Год: <b>{data.get('year')}</b>\n")
        summary.append(f"Пожалуйста, подтвердите выбор")

        await message.answer("\n".join(summary), parse_mode="HTML", reply_markup=kb)
        await StartNewStates.confirmation.set()
        return

    categories = get_categories()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Начать заново"))
    kb.add(KeyboardButton("Без категории"))
    for c in categories:
        kb.add(KeyboardButton(c))
    await message.answer("Введите категорию:", reply_markup=kb)
    await StartNewStates.choosing_category.set()


async def start_new_category(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "начать заново":
        await start_new_handler(message, state)
        return

    if txt.startswith("Без категории"):
        await state.update_data(subcategory="")
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("Подтвердить", callback_data="sn_confirm"),
            InlineKeyboardButton("Отмена", callback_data="sn_restart"),
        )
        d = await state.get_data()
        await message.answer(
            f"Вы выбрали:\n"
            f"Вид справки: <b>По стране</b>\n"
            f"Страна-партнёр: <b>{d.get('partner')}</b>\n"
            f"Год: <b>{d.get('year')}</b>\n"
            f"Категория: <b>Нет категории</b>\n\n"
            f"Пожалуйста, подтвердите выбор",
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
        f"Вы выбрали:\n"
        f"Вид справки: <b>По стране</b>\n"
        f"Страна-партнёр: <b>{d.get('partner')}</b>\n"
        f"Год: <b>{d.get('year')}</b>\n"
        f"Категория: <b>{d.get('category_parent')}</b>\n"
        f"Подкатегория: <b>{d.get('subcategory')}</b>\n\n"
        f"Пожалуйста, подтвердите выбор",
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

    partner = str(d["partner"])
    year = int(d["year"])
    tn_ved = ((d.get("tn_ved")).strip() or None)
    subcategory = (d.get("subcategory") or None)
    long_report=0
    if tn_ved:
        subcategory = None
        long_report=1
    plain=int(d.get("plain") or 0)

    if isinstance(msg_or_cbq, types.CallbackQuery):
        await msg_or_cbq.message.answer("❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup=ReplyKeyboardRemove())
    else:
        await msg_or_cbq.answer("❗Идет генерация справки. Пожалуйста, подождите.❗", reply_markup=ReplyKeyboardRemove())

    try:
        res = generate_trade_document(
            region="Республика Казахстан",
            country_or_group=partner,
            start_year=None,
            end_year=year,
            digit=4,
            category=subcategory,
            text_size=7,
            table_size=25,
            country_table_size=15,
            tn_ved=tn_ved,
            month_range_raw="",
            exclude_raw=excluded_tnveds_string,
            long_report=long_report,
            plain=plain,
            include_regions=0,
            change_color=1,
        )
        
    except Exception as e:
        print(f"\n!!! oh no, error occured:\n{e}\n\n")
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer("Произошла ошибка при генерации файла. Чтобы начать заново, нажмите /start")
            await state.finish()
        else:
            await msg_or_cbq.answer("Произошла ошибка при генерации файла. Чтобы начать заново, нажмите /start")
            await state.finish()
        return

    if res["status"] != 'no_data':
        doc = res["doc"]
        filename = res["filename"]
        short_filename = res["short_filename"]
        buf = BytesIO(); doc.save(buf); buf.seek(0)
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer_document((short_filename, buf))
            await msg_or_cbq.message.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите /start")
        else:
            await msg_or_cbq.answer_document((short_filename, buf))
            await msg_or_cbq.answer(f"Ваш документ {filename} готов. Чтобы начать заново, нажмите /start")
        
        no_tn_ved = "None"
        no_subcategory = "None"
        no_plain = "None"
        if tn_ved:
            no_tn_ved = tn_ved
        if subcategory:
            no_subcategory = subcategory
        if tn_ved !=0:
            no_plain = "Самолётик"
        hist_txt = partner +' '+ no_tn_ved +' '+ no_subcategory +' '+ no_plain
        print(hist_txt)
        await add_download_history(telegram_id, hist_txt, year)
        await state.finish()
    else:
        if isinstance(msg_or_cbq, types.CallbackQuery):
            await msg_or_cbq.message.answer("По выбранным фильтрам нет данных. Чтобы начать заново, нажмите /start")
        else:
            await msg_or_cbq.answer("По выбранным фильтрам нет данных. Чтобы начать заново, нажмите /start")
        await state.finish()
