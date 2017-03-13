# pdfs
Скрипт для сбора ссылок на pdf документы с разных поисковиков: 
'https://duckduckgo.com/'
'https://bing.com/'
'http://www.ask.com/'
'https://www.yandex.ru/'
'https://www.yahoo.com/'
на выбор.

# Перед началом работы необходимо:
1. Установить Python3.
2. Настроить и активировать virtual environment для python3.
3. Установить все библиотеки из файла requirements.txt.
4. Прописать настройки для подключения к базе данных:
```python
DATABASE = {
	'drivername': 'postgres',
	'database': '*',
	'host': '*',
	'port': '*',
	'username': '*',
	'password': '*'
}
DBX_KEY = '*'
```
в двух файлах: в flags_pdf_search.py (для запуска из terminal) и в pdfs_search.py.
# Для запуска сбора pdf-ссылок нужно:
1. Подготовить список ключевых слов:
либо
В google spreadsheet: список ключевых слов, в виде таблицы с одной колонкой. Название колонки = 'keywords'.
Опубликовать файл в формате CSV.
либо
В csv файле с ключевыми словами.
либо
В базе данных, где список ключевых слов находится в колонке = 'keyword'
2. Запустить скрипт одной из нескольких команд в зависимости от формата файла с ключевыми словами:
  * google spreadsheet: flags_pdf_search.py -l '#google_sheets_csv_url_here#' -s '#one_of_aforementioned_seach_engines_here'
  * csv: flags_pdf_search.py -c '#path_to_the_csv_file_here#' -s '#one_of_aforementioned_seach_engines_here'
  * database: flags_pdf_search.py -o '#databese_host_here#' -n '#database_name_here' -u '#database_user_name_here' -p '#database_password_here' -s '#one_of_aforementioned_seach_engines_here'
