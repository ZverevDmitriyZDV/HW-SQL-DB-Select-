rtist_list = 'Horse the band,Slipknot,Numenorean,The Dillinger Escape Plan,Polythia,Mashmello,' \
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