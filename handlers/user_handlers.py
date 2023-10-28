from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import CommandStart

from config_data.config import Config, load_config
from utils.grouping import aggregation_data

from datetime import datetime


router = Router()
config: Config = load_config('.env')


@router.message(CommandStart())
async def process_start_command(message: Message):
    await message.answer(text="Это бот для тестового задания. Отправь текст в виде json")


@router.message(F.text.startswith('{'))
async def send_echo(message: Message):
    result = aggregation_data(message.text)
    print(result)
    await message.answer(result)
