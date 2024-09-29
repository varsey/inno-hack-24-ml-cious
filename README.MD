Для запуска через docker-compose:
* Положить файлы main1.csv, main2.csv, main3.csv в директорию data
* Из корня проекта ``docker build -t solution .``
* Из корня проекта ``docker compose -f docker-compose/docker-compose.yaml up``
* Дождаться пока данные зальются в кх и запуститься solution до сообщения solution-1 exited with code 0

Для запуска локально:
* Установить виртуальное окружение и зависимости из requirement.txt
* Положить файлы main1.csv, main2.csv, main3.csv в директорию data
* Запустить сервис clickhouse из докер компоуз (``docker compose -f docker-compose/docker-compose.yaml up``)
* Поменять в модуле клиента кх ``Client(host='clickhouse') на 'localhost'``
* Запустить из корня проекта python3 main.py