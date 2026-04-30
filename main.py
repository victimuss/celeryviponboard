from fastapi import FastAPI
import redis
from celery import chain
from dotenv import load_dotenv
import os
from worker import generate_vip_card, unstable_upload, send_email

# Загружаем переменные окружения из файла .env (например, строку подключения к Redis)
load_dotenv()

# Инициализируем приложение FastAPI
app = FastAPI(title="Black Friday Sale (with Celery)")

# Получаем URL нашего брокера (Redis) из переменных окружения
REDIS_URL = os.getenv("REDIS_URL")
db = None 

# Событие старта приложения FastAPI
@app.on_event("startup")
def startup_event():
    global db
    # Подключаемся к Redis для проверки соединения при старте
    db = redis.from_url(REDIS_URL, decode_responses=True)
    db.ping()

# Эндпоинт для регистрации нового пользователя
@app.post("/onboarding/{username}")
def onboard_user(username: str):
    # Celery Chain: позволяет выстроить задачи в последовательную цепочку (pipeline).
    # Результат выполнения первой задачи автоматически передается на вход второй и так далее.
    # Метод .s() создает "сигнатуру" (signature) задачи — то есть подготавливает её к вызову.
    workflow = chain(
        generate_vip_card.s(username),  # Шаг 1: генерируем карту
        unstable_upload.s(),            # Шаг 2: загружаем в "облако"
        send_email.s()                  # Шаг 3: отправляем письмо
    )
    
    # Запускаем всю цепочку асинхронно в фоне! 
    # FastAPI не ждет ее завершения и сразу переходит к следующей строке.
    workflow.apply_async() 
    
    # Мгновенно возвращаем ответ пользователю, пока Celery делает тяжелую работу
    return {
        "status": "✅ УСПЕХ", 
        "message": f"Привет, {username}! Твоя VIP-карта генерируется. Ссылка скоро придет."
    }