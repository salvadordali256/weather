import sqlite3

conn = sqlite3.connect('./northwoods_full_history.db')
cursor = conn.cursor()
cursor.execute('SELECT name, latitude, longitude FROM stations ORDER BY name')

print('\nAvailable Stations:')
print('-' * 60)
for row in cursor.fetchall():
    print(f'{row[0]:<20} ({row[1]:.2f}, {row[2]:.2f})')

conn.close()
