from aiogram.dispatcher.filters.state import State, StatesGroup


class StartNewStates(StatesGroup):
    choosing_variant = State()
    waiting_for_tnved = State()
    choosing_partner = State()
    choosing_year = State()
    choosing_category = State()
    choosing_subcategory = State()
    confirmation = State()
    waiting_for_access_data = State()
