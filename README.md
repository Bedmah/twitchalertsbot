# New Allbot (SQLite)

## Главное
- Подписка на любого стримера по логину или ссылке Twitch
- Рекомендации (преднастроенный список)
- Мои подписки
- Ссылки
- Обратная связь
- Админ-панель (пользователи, логи, рассылка текст/фото/файл)
- Кулдаун уведомлений о перезапуске стрима: 30 минут (настраивается)

## База данных
- `bd/oldBD/oldbot.db` — мигрированные старые данные
- `bd/BD/bot.db` — новая чистая тестовая БД (активная)

Legacy-данные переносятся из:
- `../users`
- `../subscribers.json`
- `../admins.json`
- (и при наличии) `data/users`, `data/subscribers.json`, `data/admins.json`

## Установка
```bash
pip install -r requirements.txt
```

## Запуск
```bash
python bot.py
```
или
```powershell
powershell -ExecutionPolicy Bypass -File run.ps1 start
```

## run.ps1
- `start`
- `stop`
- `restart`
- `status`
- `logs -Tail 80`

## Важно
Текущие токены перенесены в `.env` для миграции. Рекомендуется перевыпустить ключи (rotate).
