import json
import boto3
import logging


MOVIE_TABLE = 'movies'
TV_SHOW_TABLE = 'tv-shows'
EPISODE_TABLE = 'episodes'

S3_BUCKET = 'video-content-bucket-1'
JSON_FILE = 'contentFeed.json'


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
movie_table = dynamodb.Table(MOVIE_TABLE)
tv_show_table = dynamodb.Table(TV_SHOW_TABLE)
episode_table = dynamodb.Table(EPISODE_TABLE)


def lambda_handler(event, context):
    movie_records = get_all_dynamo_records(movie_table)['Items']
    format_movie_data(movie_records)





def format_movie_data(movie_dynamo_data):
    formatted_movie_list = []

    for movie in movie_dynamo_data:
        formatted_movie = {
            "title": movie["name"],
			"longDescription": movie["description"],
			"thumbnail": movie["thumbnailUrl"],
			"releaseDate": movie["year"],
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

        formatted_movie_list.append(formatted_movie)
        print(formatted_movie_list[0])
        return formatted_movie_list


def get_all_dynamo_records(table):
    try:
        records = table.scan()
        return records
    except Exception as ex:
        logger.error(f"Error retrieving all records from table {table}. Exception: {ex}")
        raise