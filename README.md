DAG в airflow
Отчётность по работе приложения за предыдущий день.
Параллельно обрабатывается две таблицы, feed_actions - активность юзеров в ленте, message_actions - активность юзеров в мессенджере. 
1. В feed_actions для каждого юзера считается число просмотров и лайков контента. 
2. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. 
3. Две таблицы объединяются в одну.
3. Для этой таблицы считаются все эти метрики в разрезе по полу, возрасту и ОС.
4. Финальную данные со всеми метриками записываем в отдельную таблицу в ClickHouse.