import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import date
from enum import Enum


MOVIE_TABLE = 'movies'
TV_SHOW_TABLE = 'tv-shows'
EPISODE_TABLE = 'episodes'

S3_BUCKET = 'video-content-bucket-1'
JSON_FILE = 'contentFeed.json'

class SpecialSeason(Enum):
    MINI_SERIES = 'Mini Series'
    EXTRAS = 'Extras'
    MOVIES = 'Movies'

NUMBER_OF_RECENTLY_ADDED_MOVIES = 10

exported_content = [
    {
        "name": "all",
        "path": "contentFeed_exported.json",
        "include_movies": True,
        "include_tv_shows": True,
        "sort_by": "dateAdded"
    },
    {
        "name": "movies",
        "path": "contentFeed_exported_movies.json",
        "include_movies": True,
        "include_tv_shows": False,
        "sort_by": "dateAdded"
    },
    {
        "name": "tv_shows",
        "path": "contentFeed_exported_tv_shows.json",
        "include_movies": False,
        "include_tv_shows": True,
        "sort_by": "dateAdded"
    },
    {
        "name": "decades",
        "path": "contentFeed_exported_decades.json",
        "include_movies": True,
        "include_tv_shows": False,
        "sort_by": "year"
    },
    {
        "name": "directors",
        "path": "contentFeed_exported_directors.json",
        "include_movies": True,
        "include_tv_shows": False,
        "sort_by": "year"
    },
    {
        "name": "trending",
        "path": "contentFeed_exported_trending.json",
        "include_movies": True,
        "include_tv_shows": False,
        "sort_by": "views"
    }
]


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
movie_table = dynamodb.Table(MOVIE_TABLE)
tv_show_table = dynamodb.Table(TV_SHOW_TABLE)
episode_table = dynamodb.Table(EPISODE_TABLE)


def lambda_handler(event, context):
    print("Export started.")

    for content in exported_content:
        if content['include_movies'] == True:
            movie_records = get_all_dynamo_records(movie_table)
            sorted_movie_records = sorted(movie_records, key=lambda x: x[content['sort_by']])
            movies = format_movie_data(sorted_movie_records, content['name'])
        if content['include_tv_shows'] == True:
            tv_show_records = get_all_dynamo_records(tv_show_table)
            sorted_tv_show_records = sorted(tv_show_records, key=lambda x: x[content['sort_by']])
            tv_shows = format_tv_show_data(sorted_tv_show_records, content['name'])

        path = content['path']

        if content['include_movies'] and content['include_tv_shows']:
            combined_data = {
                "TV Shows": tv_shows,
                "Movies": movies
            }
        elif content['include_movies']:
            combined_data = {
                "Movies": movies
            }
        elif content['include_tv_shows']:
            combined_data = {
                "TV Shows": tv_shows,
            }

        s3.put_object(
            Body=json.dumps(combined_data),
            Bucket=S3_BUCKET,
            Key=path
        )

        print(f"Export for {content['name']} completed.")

    print("Export completed.")

def format_movie_data(movie_dynamo_data, name):
    print(f"Movie formatting for {name} started.")  
    formatted_movie_list = []

    formatted_movie_list_length = len(movie_dynamo_data)
    cutoff = formatted_movie_list_length - NUMBER_OF_RECENTLY_ADDED_MOVIES

    for i, movie in enumerate(movie_dynamo_data):

        if i < cutoff:
            formatted_movie = {
                "title": movie["name"],
                "longDescription": movie["description"],
                "thumbnail": movie["thumbnailUrl"],
                "releaseDate": movie["year"],
                "dateAdded": movie["dateAdded"],
                "rating": movie["rating"],
                "cast": movie["cast"],
                "director": movie["director"],
                "genres": movie["genres"],
                "content": {
                    "duration": int(movie["duration"]),
                    "videos": [{
                        "videoType": movie["videoType"],
                        "url": movie["videoUrl"],
                    }]
                },
                "trailerUrl": movie["trailerUrl"] if movie["trailerUrl"] else "",
                "dateAdded": movie["dateAdded"],
                "lastWatched": movie["lastWatched"] if movie["lastWatched"] else "",
                "views": int(movie["views"])
            }
        else:
            # Make copy of list in order to append
            genres = movie["genres"][:]
            genres.append("Recently Added")

            formatted_movie = {
                "title": movie["name"],
                "longDescription": movie["description"],
                "thumbnail": movie["thumbnailUrl"],
                "releaseDate": movie["year"],
                "dateAdded": movie["dateAdded"],
                "rating": movie["rating"],
                "cast": movie["cast"],
                "director": movie["director"],
                "genres": genres,
                "content": {
                    "duration": int(movie["duration"]),
                    "videos": [{
                        "videoType": movie["videoType"],
                        "url": movie["videoUrl"],
                    }]
                },
                "trailerUrl": movie["trailerUrl"] if movie["trailerUrl"] else "",
                "dateAdded": movie["dateAdded"],
                "lastWatched": movie["lastWatched"] if movie["lastWatched"] else "",
                "views": int(movie["views"])
            }

        if name == 'decades':
           formatted_movie['genres'] = [movie["year"][:3] + "0s"]

        if name == 'trending':
            # Getting the anniversary year genres
            formatted_movie['genres'] = get_trending_genres(formatted_movie)
        
        if name == 'directors':
            formatted_movie['genres'] = [movie["director"]]
        
        formatted_movie_list.append(formatted_movie)


    if name == 'trending':
        # --- Tag top 10 as "Most Watched - All Time" ---
        most_watched_movies = sorted(formatted_movie_list, key=lambda m: m["views"], reverse=True)[:10]
        for movie in most_watched_movies:
            genres = movie["genres"][:]
            genres.append("Most Watched - All Time")
            movie["genres"] = genres

        # --- Sort the entire list by views (highest → lowest) ---
        formatted_movie_list = sorted(formatted_movie_list, key=lambda m: m["views"], reverse=True)

    # --- Reverse the last x items for Recently Added ---
    if len(formatted_movie_list) >= NUMBER_OF_RECENTLY_ADDED_MOVIES:
        formatted_movie_list = (
            formatted_movie_list[:-NUMBER_OF_RECENTLY_ADDED_MOVIES] +
            list(reversed(formatted_movie_list[-NUMBER_OF_RECENTLY_ADDED_MOVIES:]))
        )

    print(f"Movie formatting for {name} completed.")  
    return formatted_movie_list


def format_tv_show_data(tv_show_dynamo_data, name):
    print(f"TV Show formatting for {name} started.")   
    formatted_tv_show_list = []

    for tv_show in tv_show_dynamo_data:
        formatted_tv_show = {
			"title": tv_show["name"],
			"shortDescription": tv_show["description"],
			"thumbnail": tv_show["thumbnailUrl"],
			"releaseDate": tv_show["releaseDate"],
            "firstAired": tv_show["firstAired"],
			"rating": tv_show["rating"],
			"cast": tv_show["cast"],
			"director": tv_show["director"],
			"genres": tv_show["genres"],
            "dateAdded": tv_show["dateAdded"],
            "lastWatched": tv_show["lastWatched"] if tv_show["lastWatched"] else "",
            "views": int(tv_show["views"]),
			"seasons": format_episode_data(tv_show["name"], tv_show["numberOfSeasons"])
		}

        formatted_tv_show_list.append(formatted_tv_show)

    print(f"TV Show formatting for {name} completed.")
    return formatted_tv_show_list


def format_episode_data(tv_show_name, number_of_seasons):
    formatted_seasons = []
    number_of_normal_seasons = number_of_seasons
    movies_season = None
    extras_season = None

    # Mini Series should be before normal seasons
    if special_season_exists(tv_show_name, SpecialSeason.MINI_SERIES.value):
        mini_series_season = get_special_season_episodes(tv_show_name, SpecialSeason.MINI_SERIES.value)
        formatted_seasons.append(mini_series_season)
        number_of_normal_seasons = number_of_normal_seasons - 1
    elif special_season_exists(tv_show_name, SpecialSeason.MOVIES.value):
        movies_season = get_special_season_episodes(tv_show_name, SpecialSeason.MOVIES.value)
        number_of_normal_seasons = number_of_normal_seasons - 1
    elif special_season_exists(tv_show_name, SpecialSeason.EXTRAS.value):
        extras_season = get_special_season_episodes(tv_show_name, SpecialSeason.EXTRAS.value)
        number_of_normal_seasons = number_of_normal_seasons - 1


    for i in range(int(number_of_normal_seasons)):
        formatted_episodes = []
        sk = i + 1
        sk_string = f"{sk:02}" # Append leading 0 if necessary

        episodes = get_dynamo_records_by_pk_and_partial_sk("tvShowName", tv_show_name, 'seasonAndEpisode', 'S'+sk_string, episode_table)

        sorted_episodes = sorted(episodes, key=lambda x: int(x['episode']))
        for episode in sorted_episodes:
            formatted_episode = {
                "title": episode["name"],
                "tvShowName": tv_show_name,
                "episodeNumber": int(episode["episode"]),
                "longDescription": episode["description"],
                "thumbnail": episode["thumbnailUrl"],
                "releaseDate": episode["releaseDate"],
                "rating": episode["rating"],
                "cast": episode["cast"],
                "director": episode["director"],
                "content": {
                    "videos": [{
                        "videoType": episode["videoType"],
                        "url": episode["videoUrl"],
                    }],
                    "duration": int(episode["duration"]),
                },
                "genres": episode["genres"],
                "dateAdded": episode["dateAdded"],
                "lastWatched": episode["lastWatched"] if episode["lastWatched"] else "",
                "views": int(episode["views"])
            }

            formatted_episodes.append(formatted_episode)
        
        season = {
            "title": str(i + 1),
            "episodes": formatted_episodes
        }

        formatted_seasons.append(season)
    
    # Movies and Extras Seasons should be after normal seasons
    if movies_season:
        formatted_seasons.append(movies_season)
    if extras_season:
        formatted_seasons.append(extras_season)

    return formatted_seasons


def get_special_season_episodes(tv_show_name, special_season_type):
    formatted_episodes = []

    episodes = get_dynamo_records_by_pk_and_partial_sk("tvShowName", tv_show_name, 'seasonAndEpisode', special_season_type, episode_table)
    sorted_episodes = sorted(episodes, key=lambda x: int(x['episode']))
    for episode in sorted_episodes:
        formatted_episode = {
            "title": episode["name"],
            "episodeNumber": int(episode["episode"]),
            "longDescription": episode["description"],
            "thumbnail": episode["thumbnailUrl"],
            "releaseDate": episode["releaseDate"],
            "rating": episode["rating"],
            "cast": episode["cast"],
            "director": episode["director"],
            "content": {
                "videos": [{
                    "videoType": episode["videoType"],
                    "url": episode["videoUrl"],
                }],
                "duration": int(episode["duration"]),
            },
            "genres": episode["genres"],
            "dateAdded": episode["dateAdded"],
            "lastWatched": episode["lastWatched"] if episode["lastWatched"] else "",
            "views": int(episode["views"])
        }

        formatted_episodes.append(formatted_episode)

    season = {
        "title": special_season_type,
        "episodes": formatted_episodes
    }
    return season


def special_season_exists(tv_show_name, special_season_type):
    response = get_dynamo_records_by_pk_and_partial_sk("tvShowName", tv_show_name, 'seasonAndEpisode', special_season_type, episode_table)
    
    if response != None and len(response) > 0:
        return True
    else:
        return False
    
def get_trending_genres(formatted_movie):
    today = date.today()
    release_year = int(formatted_movie['releaseDate'])
    years_ago = today.year - release_year

    if years_ago == 10:
        return ["10 Year Anniversary"]
    elif years_ago == 25:
        return ["25 Year Anniversary"]
    elif years_ago == 50:
        return ["50 Year Anniversary"]
    elif years_ago == 100:
        return ["100 Year Anniversary"]
    else:
        return formatted_movie['genres']



# AWS Service Functions
def get_all_dynamo_records(table):
    try:
        records = table.scan()
        return records["Items"]
    except Exception as ex:
        print(f"Error retrieving all records from table {table}. Exception: {ex}")
        raise


def get_dynamo_records_by_pk_and_partial_sk(pk_name, pk_value, sk_name, sk_value, table):
    try:
        return table.query(KeyConditionExpression=Key(pk_name).eq(pk_value) & Key(sk_name).begins_with(sk_value))["Items"]
    except Exception as ex:
        print(f"Error retrieving records with partition key {pk_value} and sort key {sk_value} from table {table}. Exception: {ex}")