# vk wall parser + selenium

Простой парсер новостей вконтакте с исользованием selenium

  - открывает вконтакте
  - 3 раза берет новости с разницов в 10 минут
  - многопоточно записывает новости в 3 файла
  - заполняет базу данных (sqlite + sqlaclhemy)