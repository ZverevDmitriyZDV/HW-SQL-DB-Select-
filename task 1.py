import sqlalchemy

db = 'postgresql://lesson2:lesson2@localhost:5432/Lesson3'
engine = sqlalchemy.create_engine(db)
connection = engine.connect()

artist_list = 'Horse the band,Slipknot,Numenorean,The Dillinger Escape Plan,Polythia,Mashmello,' \
              'Svdden Death,Scooter,Timmy Trympet,Eskimo Calboy,As I Lay Dying,Silent Planet,' \
              'All That Remains,Norma Jean,Slipknot,SUM41,Nekrogoblikon'.lower()
genres_list = 'Nintendo core,Nintendo core,New-Metal,melodic-black-Metal,Jazz-core,Math-Metal,' \
              'EDM,riddim,EDM,EDM,post-hardcore,metal-Core,post-hardcore,metal-Core,post-Hardcore,' \
              'New-Metal,post-punk,death-metal'.lower()
collection_dict = {
    'aggressive': 2005,
    'hate': 2010,
    'self-distraction': 2015,
    'drunk-dance': 2020,
    'loneliness': 2007,
    'guitar': 2012,
    'dance': 2017,
    'religion': 2019
}
artist_to_genres = {
    'Horse the band': ['nintendo core'],
    'Slipknot': ['new-metal'],
    'Numenorean': ['melodic-black-metal'],
    'The Dillinger Escape Plan': ['Jazz-core'],
    'Polythia': ['Math-Metal'],
    'Mashmello': ['EDM', 'riddim'],
    'Scooter': ['EDM'],
    'Eskimo Calboy': ['EDM', 'post-hardcore'],
    'As I Lay Dying': ['metal-Core'],
    'Silent Planet': ['post-Hardcore'],
    'All That Remains': ['metal-Core'],
    'Norma Jean': ['post-Hardcore'],
    'Slipknot': ['New-Metal'],
    'SUM41': ['post-punk'],
    'Nekrogoblikon': ['death-metal'],
    'Svdden Death': ['EDM', 'riddim'],
    'Timmy Trympet': ['EDM']
}
album_dict = {
    'Horse the band':
        {'a Natural Death': 2009,
         'A Reason to Live': 2010},
    'Slipknot':
        {'IOWA': 2000,
         'Vol3': 2004},
    'Numenorean':
        {'Adore': 2019},
    'The Dillinger Escape Plan':
        {'Milk Lizard': 2015},
    'Polythia':
        {'Inferno': 2020},
    'Mashmello':
        {'Crusade': 2018},
    'Svdden Death':
        {'Crusade': 2018},
    'Scooter':
        {'Paul is Dead': 2019},
    'Timmy Trympet':
        {'Paul is Dead': 2019},
    'Eskimo Calboy':
        {'The Scene': 2020},
    'As I Lay Dying':
        {'The Powerless Rise': 2019},
    'Silent Planet':
        {'Trilogy': 2010},
    'All That Remains':
        {'The Fall of Ideals': 2009},
    'Norma Jean':
        {'Wrongdoers': 2005},
    'SUM41':
        {'Does This Look Infected?': 2000},
    'Nekrogoblikon':
        {'Welcome to Bonkers': 2014}
}

songs_dict = {
    'a Natural Death':
        {'Face of bear': 230},
    'A Reason to Live':
        {'Your Fault': 210},
    'IOWA':
        {'My Plague': 240},
    'Vol3':
        {'The Nameless': 300},
    'Adore':
        {'Adore': 480},
    'Milk Lizard':
        {'Milk Lizard': 270},
    'Inferno':
        {'Inferno': 115},
    'Crusade':
        {'Crusade': 280},
    'Paul is Dead':
        {'Paul is Dead': 205},
    'The Scene':
        {'Hypa, Hypa': 295},
    'The Powerless Rise':
        {'Without Conclusion': 325},
    'Trilogy':
        {'Trilogy': 225},
    'The Fall of Ideals':
        {'This Calling': 310},
    'Wrongdoers':
        {'Wrongdoers': 285},
    'Does This Look Infected?':
        {'Still Waiting': 175},
    'Welcome to Bonkers':
        {'Darkness': 272}
}

collection_to_song_dict = {
    'Face of bear': ['aggressive'],
    'Your Fault': ['aggressive', 'self-distraction'],
    'My Plague': ['hate', 'self-distraction'],
    'Adore': ['drunk-dance'],
    'Milk Lizard': ['guitar', 'loneliness'],
    'Inferno': ['dance', 'guitar'],
    'Crusade': ['dance', 'drunk-dance'],
    'Paul is dead': ['dance', 'drunk-dance'],
    'Hypa, Hypa': ['dance', 'drunk-dance', 'guitar'],
    'Without Conclusion': ['religion', 'hate'],
    'Trilogy': ['self-distraction'],
    'This Calling': ['aggressive'],
    'Wrongdoers': ['religion', 'hate', 'loneliness'],
    'The Nameless': ['hate'],
    'Still Waiting': ['drunk-dance'],
    'Darkness': ['religion', 'self-distraction'],

}


def get_id(cell_name, table_name, cell_key, key_word):
    return connection.execute(f'''SELECT {cell_name} FROM {table_name} 
                                                WHERE {cell_key}='{key_word}'
                                            ''').fetchall()


def cell_is_exist(name_cell, table_name, cell_key, addition=None):
    if addition is None:
        return connection.execute(f'''SELECT {name_cell} FROM {table_name}
                                                WHERE {name_cell}= '{cell_key}'
                                            ''').fetchall()
    else:
        return connection.execute(f'''SELECT {name_cell} FROM {table_name}
                                                WHERE {name_cell}= '{cell_key}'
                                                AND {addition}
                                            ''').fetchall()


def insert_info(table_name, cell_list, value_insert, type='info'):
    if type == 'info':
        value = f"'{value_insert}'"
    else:
        value = value_insert
    connection.execute(f'''INSERT INTO {table_name}({cell_list})
                           VALUES({value.lower()});
                    ''')


def fill_one_cell_table(list_format, table_name, key_cell):
    for elem_of_list in list_format.split(','):
        if not cell_is_exist(key_cell, table_name, elem_of_list):
            insert_info(table_name, key_cell, elem_of_list)
    return connection.execute(f'''SELECT * FROM {table_name}
                ''').fetchall()


def fill_album_table(dict):
    for key in dict.keys():
        for name in dict[key].keys():
            if not cell_is_exist('Name', 'Albums', name):
                value_insert = f"'{name}', {dict[key][name]}"
                insert_info('Albums', 'Name, PublishedYear', value_insert, type='text')
    return connection.execute(f'''SELECT * FROM Albums
                ''').fetchall()


def fill_song_table(dict):
    for album in dict.keys():
        album_id = get_id('Id', 'Albums', 'Name', album.lower())[0][0]
        for song_name in dict[album].keys():
            if not cell_is_exist('Name', 'Songs', song_name, f'AlbumId = {album_id}'):
                value_insert = f"'{song_name}', {dict[album][song_name]},{album_id}"
                insert_info('Songs', 'Name, Length, AlbumId', value_insert, type='text')
    return connection.execute(f'''SELECT * FROM Songs
                ''').fetchall()


def fill_style_table(dict):
    for style in dict:
        if not cell_is_exist('Name', 'Collection', style, f'PublishedYear = {dict[style]}'):
            insert_info('Collection', 'Name,PublishedYear', f"'{style}',{dict[style]}", type='text')
    return connection.execute(f'''SELECT * FROM Collection
                ''').fetchall()


def is_list(dict):
    if isinstance(dict, list):
        return dict
    else:
        return dict.keys()


def fill_table_to_table(dict, table_result, table_name1, table_name2, cell_table1, cell_table2):
    for key in dict.keys():
        first_id = get_id('Id', table_name1, 'Name', key.lower())[0][0]
        for value in is_list(dict[key]):
            second_id = get_id('Id', table_name2, 'Name', value.lower())[0][0]
            if not cell_is_exist(cell_table1, table_result, first_id, f'{cell_table2} = {second_id}'):
                connection.execute(f'''INSERT INTO {table_result}({cell_table1},{cell_table2})
                                       VALUES({first_id},{second_id});
                                       ''')
    return connection.execute(f'''SELECT * FROM {table_result}
                ''').fetchall()


def purge_all():
    clean_table('SongCollection')
    clean_table('ArtistsAlbum')
    clean_table('ArtistsGenres')
    clean_table('Collection')
    clean_table('Songs')
    clean_table('Albums')
    clean_table('Genres')
    clean_table('Artists')


def clean_table(table_name):
    connection.execute(f'''DELETE FROM {table_name}''')


if __name__ == '__main__':
    purge_all()
    fill_one_cell_table(artist_list, 'Artists', 'Name')
    fill_one_cell_table(genres_list, 'Genres', 'Name')
    fill_album_table(album_dict)
    fill_song_table(songs_dict)
    fill_style_table(collection_dict)
    fill_table_to_table(
        artist_to_genres,
        'ArtistsGenres',
        'Artists',
        'Genres',
        'ArtistId',
        'GenresId'
    )
    fill_table_to_table(
        album_dict,
        'ArtistsAlbum',
        'Artists',
        'Albums',
        'ArtistId',
        'AlbumId'
    )
    fill_table_to_table(
        collection_to_song_dict,
        'SongCollection',
        'Songs',
        'Collection',
        'SongId',
        'CollectionId')
