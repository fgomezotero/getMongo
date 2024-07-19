#!/usr/bin/env python3

# Implement an script getMongo to replace Nifi processor GetMongo and load query in json format from file
import pymongo, click, json
from bson import ObjectId
import re


def convert_query_to_objectid(query_dict):
    """
    Converts nested ObjectIds in a query dictionary to bson.ObjectId instances using regex.

    Args:
        query_dict: A dictionary representing the MongoDB query.

    Returns:
        A dictionary with converted ObjectId instances.
    """
    if isinstance(query_dict, str):
        parsing = re.match(r"ObjectId\('(.*?)'\)", query_dict)
        return ObjectId(parsing.group(1)) if parsing else value
    elif isinstance(query_dict, dict):
        return {
            key: convert_query_to_objectid(value) for key, value in query_dict.items()
        }
    elif isinstance(query_dict, list):
        return [convert_query_to_objectid(item) for item in query_dict]
    else:
        return query_dict


@click.command
@click.option("--host", "-h", help="MongoDB host")
@click.option("--port", "-p", default=27017, type=int, help="MongoDB port")
@click.option("--database", "-d", help="MongoDB database")
@click.option("--collection", "-c", help="MongoDB collection")
@click.option(
    "--query_file", "-q", type=click.Path(exists=True), help="MongoDB query file path"
)
@click.option(
    "--projection",
    "-j",
    help="MongoDB projection, '{\"key\": int}' int value is 1 for inclusion, 0 for exclusion",
)
@click.option(
    "--limit",
    "-l",
    default=0,
    type=int,
    help="Limit the number of results. Defaults to 0",
)
@click.option("--username", "-u", help="MongoDB username")
@click.option("--password", "-s", help="MongoDB password")
@click.option(
    "--authentication_database",
    "-a",
    help="MongoDB authentication database",
)
# @click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def get_mongo_data(
    host: str,
    port: int,
    database: str,
    collection: str,
    query_file: str,
    projection: dict[str:int],
    limit: int = 0,
    username: str = None,
    password: str = None,
    authentication_database: str = None,
) -> list:
    """Ejecuta consultas sobre instancias de MongoDB

    Args:
        host (str): MongoDB host.
        port (int): MongoDB post.
        database (str): MongoDB database.
        collection (str): MongoDB collection.
        query_file (str): MongoDB query file path.
        projection (dict[str:int]): MongoDB projection, \'{\"key\": int}\' int value is 1 for inclusion, 0 for exclusion.
        limit (int, optional): Limit the number of results. Defaults to 0.
        username (str, optional): MongoDB username. Defaults to None.
        password (str, optional): MongoDB password. Defaults to None.
        authentication_database (str, optional): MongoDB authentication database. Defaults to None.

    Returns:
        list: Diccionario de resultados de la consulta
    """
    try:
        # Connect to the MongoDB server
        if username and password:
            conn = pymongo.MongoClient(
                f"mongodb://{username}:{password}@{host}:{port}/{authentication_database}"
            )
        else:
            conn = pymongo.MongoClient(f"mongodb://{host}:{port}")

        # Get the database and collection
        db = conn[database]
        coll = db[collection]

        # Read the query from the JSON file
        with open(query_file, "r") as f:
            query_string = json.load(f)

        # Convert string to dictionary and handle ObjectIds
        query = convert_query_to_objectid(query_string)

        # click.echo(query)

        projection = json.loads(projection) if projection else None
        # Execute the query
        cursor = coll.find(query, projection)

        # Apply the limit if provided
        if limit > 0:
            cursor = cursor.limit(limit)

        # Convert the cursor to a list of dictionaries
        result = list(cursor)

        # Print the result
        click.echo(result)

    except pymongo.errors.ConnectionFailure as e:
        raise ConnectionError(f"Connection to MongoDB server failed: {e}") from e
    except pymongo.errors.PyMongoError as e:
        raise Exception(f"An error occurred during MongoDB operation: {e}") from e
    except json.decoder.JSONDecodeError as e:
        raise Exception(
            "Invalid JSON input: Check the query file format or the projection parameters must be enclosed in simple quotes. Example: '{\"...\"}'"
        ) from e
    finally:
        # Ensure connection is closed even on errors
        if conn:
            conn.close()


# define the main call
if __name__ == "__main__":
    # Call to the main function
    get_mongo_data()
