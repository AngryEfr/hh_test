from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

from config_data.config import Config, load_config


config: Config = load_config('.env')
client = MongoClient(config.db.db_host, 27017)

collection = client['test']['sample_collection']


def aggregation_data(text):
    test = json.loads(text)

    x = ''

    dt_from = datetime.fromisoformat(test['dt_from'])
    dt_upto = datetime.fromisoformat(test['dt_upto'])

    date_list = []
    if test["group_type"].lower() == 'month':
        x = '%Y-%m-01T00:00:00'
        test["group_type"] = '%Y%m'
        date_list = [dt_from + relativedelta(months=x) for x in
                     range((dt_upto.year - dt_from.year) * 12 + (dt_upto.month - dt_from.month) + 1)]
    elif test["group_type"].lower() == "hour":
        x = '%Y-%m-%dT%H:00:00'
        test["group_type"] = "%H"
        seconds = (dt_upto - dt_from).total_seconds()
        hours, remainder = divmod(seconds, 3600)
        date_list = [dt_from + timedelta(seconds=x * 3600) for x in range(int(hours) + 1)]
    elif test["group_type"].lower() == "day":
        x = '%Y-%m-%dT00:00:00'
        test["group_type"] = "%Y-%m-%d"
        date_list = [dt_from + timedelta(days=x) for x in range((dt_upto - dt_from).days + 1)]
    elif test["group_type"].lower() == "year":
        x = '%Y-01-01T00:00:00'
        test["group_type"] = "%Y"
        date_list = [dt_from + timedelta(days=365 * x) for x in range((dt_upto.year - dt_from.year) + 1)]
    elif test["group_type"].lower() == "minute":
        x = '%Y-%m-%dT%H:%M:00'
        test["group_type"] = "%Y-%m-%d%H:%M"
        date_list = [dt_from + timedelta(minutes=x) for x in range((dt_upto - dt_from).seconds // 60 + 1)]
    else:
        x = '%Y-%m-%dT%H:%M:%S'
        test["group_type"] = "%Y-%m-%dT%H:%M:%S"
        date_list = [dt_from + timedelta(seconds=x) for x in range((dt_upto - dt_from).seconds + 1)]

    pipeline = [
        # Фильтруем документы по диапазону дат
        {'$match': {
            'dt': {'$gte': dt_from, '$lte': dt_upto},
        }},

        # Сгруппируем по дате и подсчитаем сумму значений
        {'$group': {
            '_id': {'$dateToString': {'format': f'{x}', 'date': '$dt'}},
            'totalValue': {'$sum': '$value'},
        }},

        # Добавляем пропущенные даты с нулевым значением
        {'$unionWith': {
            'coll': 'null_dates_collection',
            'pipeline': [{'$match': {'_id': {'$gte': dt_from, '$lte': dt_upto}}}],
        }},

        # Сортируем результаты по дате
        {'$sort': {'_id': 1}},
    ]

    result = collection.aggregate(pipeline)

    dataset = []
    labels = []

    for doc in result:
        dataset.append(doc['totalValue'])
        labels.append(doc['_id'])

    formatted_dates = []

    for date in date_list:
        # Преобразуем дату в строку в нужном формате
        date_string = date.strftime("%Y-%m-%dT%H:%M:%S")
        # Добавляем строку в новый список
        formatted_dates.append(date_string)

    for formatted_date in formatted_dates:

        if formatted_date not in labels:
            labels.insert(formatted_dates.index(formatted_date), formatted_date)
            dataset.insert(formatted_dates.index(formatted_date), 0)

    output = {"dataset": dataset, "labels": labels}
    output = json.dumps(output)

    # Выводим результат
    return(output)
