import sqlalchemy

db = 'postgresql://lesson2:lesson2@localhost:5432/Lesson3'
engine = sqlalchemy.create_engine(db)
connection = engine.connect()


def find_album_year(year):
    result_list = connection.execute(f'''SELECT Name, PublishedYear FROM ALbums
                                          WHERE PublishedYear >= {year}
    ''').fetchall()
    result_line = f'Список альбомов вышедних после {year}\n'
    for album in result_list:
        result_line += f'{album[0].capitalize()} :год издания {album[1]}\n'
    return result_line


def find_longest_song():
    result_list = connection.execute(f''' SELECT Name,Length FROM Songs
                            ORDER BY Length  DESC
    ''').fetchall()
    return f'Самая длинная песня {result_list[0][0]}, продолжительностью {result_list[0][1]} ceк\n'


def find_song_by_lenght(lenght):
    result_list = connection.execute(f''' SELECT Name,Length FROM Songs
                            WHERE Length >= {lenght}
    ''').fetchall()
    result_line = f'Список песен длиннее {lenght}\n'
    for song in result_list:
        result_line += f'{song[0].capitalize()} :длительность {song[1]}\n'
    return result_line


def find_collection_year(year):
    result_list = connection.execute(f'''SELECT Name, PublishedYear FROM Collection
                                          WHERE PublishedYear BETWEEN {year[0]} AND {year[1]}
    ''').fetchall()
    result_line = f'Список сборников, вышедних в период {year[0]}-{year[1]}\n'
    for collection in result_list:
        result_line += f'{collection[0].capitalize()}\n'
    return result_line


def find_one_word_name():
    result_list = connection.execute('''SELECT Name FROM Artists
                                        WHERE Name NOT LIKE '%% %%'
    ''').fetchall()
    result_line = f'Список артистов, у которых одно слово в имени \n'
    for name in result_list:
        result_line += f'{name[0].capitalize()}\n'
    return result_line


def find_song_with(part):
    result_list = connection.execute(f'''SELECT Name FROM Songs
                                        WHERE Name LIKE '%%{part}%%'
    ''').fetchall()
    result_line = f'Список песен, у которых в имени {part} \n'
    for name in result_list:
        result_line += f'{name[0].capitalize()}\n'
    return result_line


print(find_album_year(2018))
print(find_longest_song())
print(find_song_by_lenght(210))
print(find_collection_year([2018, 2020]))
print(find_one_word_name())
print(find_song_with('my '))
