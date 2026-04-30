from celery import Celery
import time
import os
import random # ⬅️ Не забываем импорт!
from dotenv import load_dotenv

# Загружаем переменные окружения (например, ссылку на Redis)
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL")

# Создаем экземпляр Celery. 
# Первый аргумент - имя приложения ("worker"). 
# broker - брокер сообщений (в нашем случае Redis), который принимает задачи от FastAPI и передает воркеру.
celery_app = Celery("worker", broker=REDIS_URL)

# Настраиваем расписание (Celery Beat)
# Celery Beat используется для периодических задач (по аналогии с cron).
celery_app.conf.beat_schedule = {
    'give-bonus-every-minute': {
        'task': 'worker.give_daily_bonus', # Имя задачи, которую нужно вызывать
        'schedule': 60.0,                  # Интервал вызова в секундах
    },
}

# 1. Задача по расписанию (Beat)
# Декоратор @celery_app.task регистрирует функцию как фоновую задачу Celery.
@celery_app.task
def give_daily_bonus():
    # Эта задача будет выполняться каждую минуту благодаря настройкам beat_schedule выше.
    print("💎 [BEAT] ДАЕМ ЕЖЕДНЕВНЫЙ БОНУС ВСЕМ VIP-АМ!")
    return "Бонус выдан"

# 2. Шаг первый: Генерация (обычная задача, без ретраев)
@celery_app.task
def generate_vip_card(username):
    print(f"🪪 [1/3] Генерируем VIP-карту для {username}...")
    # Имитируем долгую ресурсоемкую работу (например, генерацию PDF)
    time.sleep(3)
    
    # Возвращаем "файл" (просто строку для примера).
    # В рамках цепочки (chain) этот результат АВТОМАТИЧЕСКИ полетит на вход следующей задаче!
    return f"pdf_data_for_{username}"

# 3. Шаг второй: Загрузка с имитацией обрыва связи
# ⬅️ bind=True нужен, чтобы у нас появился доступ к самому экземпляру задачи (через self).
# Это дает возможность вызывать self.retry() для повторных попыток при ошибках.
# max_retries=5 означает, что задача попробует выполниться максимум 5 раз.
@celery_app.task(bind=True, max_retries=5) 
def unstable_upload(self, file_data):
    print(f"☁️ [2/3] Пытаемся загрузить {file_data} в облако...")
    try:
        # Имитация: в 50% случаев облако недоступно
        if random.choice([True, False]):
            print("❌ ОШИБКА СЕТИ! Облако недоступно.")
            # Генерируем ошибку, чтобы сымитировать проблему с сетью
            raise ConnectionError("Cloud is down!")
            
        print("✅ Файл успешно загружен в облако!")
        # Возвращаем ссылку, которая автоматически пойдет на вход 3-му шагу
        return f"https://s3.cloud/{file_data}.pdf"
        
    except Exception as e:
        print(f"⏳ Ждем 3 секунды и пробуем снова...")
        # Уходим на ретрай: повторяем выполнение через 3 секунды (countdown=3)
        raise self.retry(exc=e, countdown=3)

# 4. Шаг третий: Отправка письма
@celery_app.task
def send_email(file_url):
    print(f"💌 [3/3] Письмо со ссылкой {file_url} отправлено пользователю!")
    return "Цепочка завершена!"