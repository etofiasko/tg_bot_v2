from aiogram import Bot, Dispatcher, executor, types
import pandas as pd 
from config import API_TOKEN
from handlers import (
    start_handler,
    partner_chosen_handler,
    year_chosen_handler,
    confirmation_handler,
    digit_settings_handler,
    category_settings_handler,
    subcategory_settings_handler,
    months_settings_handler,
    exclude_tnved_settings_handler,
    table_size_settings_handler,
    country_table_size_settings_handler,
    text_size_settings_handler,
    access_settings_handler,
    handle_access_data,
    download_history_handler,
    start_new_handler,
    start_new_variant_chosen,
    start_new_waiting_tnved,
    start_new_partner,
    start_new_year,
    start_new_category,
    start_new_subcategory,
    start_new_confirmation,)
from aiogram.types import Message
from states import ReportStates, StartNewStates
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from bot_db import setup_users_tables


setup_users_tables()


bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())


@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: Message, state):
    await start_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_partner)
async def process_partner(message: Message, state: FSMContext):
    await partner_chosen_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_year)
async def process_year(message: Message, state: FSMContext):
    await year_chosen_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_category_settings)
async def process_category_settings(message: Message, state: FSMContext):
    await category_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_subcategory_settings)
async def process_subcategory_settings(message: types.Message, state: FSMContext):
    await subcategory_settings_handler(message, state)


@dp.callback_query_handler(state=ReportStates.confirmation)
async def process_confirmation(callback_query: types.CallbackQuery, state: FSMContext):
    await confirmation_handler(callback_query, state)


@dp.message_handler(state=ReportStates.choosing_digit_settings)
async def process_digit_settings(message: Message, state: FSMContext):
    await digit_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_months_settings)
async def process_months_settings(message: Message, state: FSMContext):
    await months_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_exclude_tnved_settings)
async def process_exclude_tnved_settings(message: Message, state: FSMContext):
    await exclude_tnved_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_table_size_settings)
async def process_table_size_settings(message: Message, state: FSMContext):
    await table_size_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_country_table_size_settings)
async def process_country_table_size_settings(message: Message, state: FSMContext):
    await country_table_size_settings_handler(message, state)


@dp.message_handler(state=ReportStates.choosing_text_size_settings)
async def process_text_size_settings(message: Message, state: FSMContext):
    await text_size_settings_handler(message, state)


@dp.message_handler(commands=['access_settings'])
async def cmd_access_settings(message: Message):
    await access_settings_handler(message)

@dp.message_handler(state=ReportStates.waiting_for_access_data)
async def process_access_settings(message: types.Message, state: FSMContext):
    await handle_access_data(message, state)

@dp.message_handler(commands=['history'])
async def cmd_history(message: types.Message):
    await download_history_handler(message)

@dp.message_handler(commands=['test'], state='*')
async def cmd_start_new(message: Message, state: FSMContext):
    await start_new_handler(message, state)

@dp.callback_query_handler(state=StartNewStates.choosing_variant)
async def cbq_start_new_variant(cbq: types.CallbackQuery, state: FSMContext):
    await start_new_variant_chosen(cbq, state)

@dp.message_handler(state=StartNewStates.waiting_for_tnved)
async def msg_start_new_tnved(message: Message, state: FSMContext):
    await start_new_waiting_tnved(message, state)

@dp.message_handler(state=StartNewStates.choosing_partner)
async def msg_start_new_partner(message: Message, state: FSMContext):
    await start_new_partner(message, state)

@dp.message_handler(state=StartNewStates.choosing_year)
async def msg_start_new_year(message: Message, state: FSMContext):
    await start_new_year(message, state)

@dp.message_handler(state=StartNewStates.choosing_category)
async def msg_start_new_category(message: Message, state: FSMContext):
    await start_new_category(message, state)

@dp.message_handler(state=StartNewStates.choosing_subcategory)
async def msg_start_new_subcategory(message: Message, state: FSMContext):
    await start_new_subcategory(message, state)

@dp.callback_query_handler(state=StartNewStates.confirmation)
async def cbq_start_new_confirmation(cbq: types.CallbackQuery, state: FSMContext):
    await start_new_confirmation(cbq, state)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
