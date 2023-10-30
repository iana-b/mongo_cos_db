import requests
from pymongo import MongoClient

# Подключаемся к MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Получаем базу данных 'cos_db'
db = client['cos_db']


def check_db_and_collection(db_name: str, collection_name: str):
    """
    Проверка наличия базы данных и коллекции. В случае их отсутствия - создает их.
    """
    if db_name in client.list_database_names():
        print(f"База данных {db_name} уже существует.")
    else:
        print(f"База данных {db_name} не найдена. Создаем...")
        client[db_name]

    if collection_name in client[db_name].list_collection_names():
        print(f"Коллекция {collection_name} уже существует.")
    else:
        print(f"Коллекция {collection_name} не найдена. Создаем...")
        client[db_name][collection_name]

    return client[db_name][collection_name]


if __name__ == '__main__':
    # Проверка наличия и создание базы данных и коллекции
    products_collection = check_db_and_collection('cos_db', 'products')
    orders_collection = check_db_and_collection('cos_db', 'orders')


def create_product(collection, product_data):
    product = {
        "_id": product_data['productId'],
        "brand": product_data['brand'],
        "name": product_data['name'],
        "price": product_data['unitPriceWithVat']
    }
    try:
        collection.insert_one(product)
    except Exception as e:
        print("An error ocurred:", e)


def create_order(collection, order_data):
    items = []
    for product in order_data['items']:
        if product['itemGroup'] == 'Product':
            item = {
                "product_id": product['productId'],
                "quantity": product['quantity']
            }
            items.append(item)

    order = {
        "_id": order_data['orderNr'],
        "date": order_data['createdAt'],
        "total_price": order_data['totalPrice'],
        "items": items,
    }
    try:
        collection.insert_one(order)
    except Exception as e:
        print("An error ocurred:", e)


def parse_notino(p_collection, o_collection):
    headers = {
        "accept": "application/json",
        # Authorization here
    }

    orders = ['357237424', '357503913', '357949892', '358044453', '358540214', '358820722', '359136818',
              '359261331', '359498364', '359654737']
    for order_id in orders:
        url = f'https://www.notino.pl/api/my/Orders/{order_id}/Detail'

        r = requests.get(url=url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            products_data = data['items']
        for product in products_data:
            if product['itemGroup'] == 'Product':
                create_product(p_collection, product)

        create_order(o_collection, data)


# parse_notino(products_collection, orders_collection)


# Функция для поиска продуктов по бренду
def find_products_by_brand(collection, brand: str):
    results = collection.find({"brand": brand})
    return list(results)


# brand_name = "Bobbi Brown"
# searched_products = find_products_by_brand(products_collection, brand_name)
# print(searched_products)


# Функция для поиска продуктов по цене
def find_products_by_price(collection, price: float):
    results = collection.find({"price": {"$gte": price}})
    return list(results)


# find_price = 150
# searched_products = find_products_by_price(products_collection, find_price)
# print(searched_products)


# Функция для определения средней стоимости заказов
def avg_order_price(collection):
    total_price_sum = 0
    total_orders = 0
    for order in collection.find():
        total_price_sum += order['total_price']
        total_orders += 1
    avg_price = total_price_sum / total_orders
    return avg_price


# average_order_price = avg_order_price(orders_collection)
# print(f"Средняя стоимость заказов: {average_order_price:.2f}")


def calculate_average_order_price(collection):
    pipeline = [
        {
            "$group": {
                "_id": None,
                "average_price": {"$avg": "$total_price"}
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["average_price"]
    else:
        return 0


# # Рассчитайте среднюю стоимость заказов
# average_order_price = calculate_average_order_price(orders_collection)
# print(f"Средняя стоимость заказов: {average_order_price:.2f}")


def total_price_without_discount(products_collection, orders_collection):
    pipeline = [
        {
            "$lookup":
                {
                    "from": "products",  # Коллекция, с которой объединяем
                    "localField": "items.product_id",  # Поле в коллекции заказов
                    "foreignField": "_id",  # Поле в коллекции продуктов
                    "as": "product"  # Новое поле, в которое будут записаны объединенные документы
                }
        },
        {
            "$unwind": "$product"
        },
        {
            "$group":
                {
                    "_id": "$_id",
                    "totalPrice": {
                        "$first": "$total_price"
                    },
                    "totalPriceWithoutDiscount": {
                        "$sum": "$product.price"
                    }
                }
        }
    ]
    results = orders_collection.aggregate(pipeline)
    return list(results)


# price_without_discount = total_price_without_discount(products_collection, orders_collection)
# print(price_without_discount)


def calculate_discount(total_price, total_price_without_discount):
    return total_price_without_discount - total_price


# Получение результатов из запроса
price_without_discount = total_price_without_discount(products_collection, orders_collection)

# Обработка результата
for order in price_without_discount:
    discount = calculate_discount(order["totalPrice"], order["totalPriceWithoutDiscount"])
    print(f"Скидка для заказа {order['_id']}: {discount:.2f}")
