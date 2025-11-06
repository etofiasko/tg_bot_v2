from aiogram.dispatcher.filters.state import State, StatesGroup


class ReportStates(StatesGroup):
    choosing_region = State()
    choosing_partner = State()
    choosing_year = State()
    confirmation = State()
    choosing_digit_settings = State()
    choosing_category_settings = State()
    choosing_subcategory_settings = State()
    choosing_months_settings = State()
    choosing_exclude_tnved_settings = State()
    choosing_table_size_settings = State()
    choosing_country_table_size_settings = State()
    choosing_text_size_settings = State()
    waiting_for_access_data = State()


class StartNewStates(StatesGroup):
    choosing_variant = State()
    waiting_for_tnved = State()
    choosing_partner = State()
    choosing_year = State()
    choosing_category = State()
    choosing_subcategory = State()
    confirmation = State()
