# Implement an script getMongo to replace Nifi processor GetMongo and load query in json format from file
import pymongo, json
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
        return ObjectId(parsing.group(1)) if parsing else query_dict
    elif isinstance(query_dict, dict):
        return {
            key: convert_query_to_objectid(value) for key, value in query_dict.items()
        }
    elif isinstance(query_dict, list):
        return [convert_query_to_objectid(item) for item in query_dict]
    else:
        return query_dict

def get_mongo_data(
    uri,
    database,
    collection,
    query_file,
    projection=None,
    limit=0,
):
    """Ejecuta consultas sobre instancias de MongoDB

    Args:
        uri (str): MongoDB URI - Connection String.
        database (str): MongoDB database.
        collection (str): MongoDB collection.
        query_file (str): MongoDB query file path.
        projection (dict[str:int]): MongoDB projection, \'{\"key\": int}\' int value is 1 for inclusion, 0 for exclusion.
        limit (int, optional): Limit the number of results. Defaults to 0.

    Returns:
        list: Diccionario de resultados de la consulta
    """
    try:
        # Connect to the MongoDB server
        conn = pymongo.MongoClient(uri)
list
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
        print(result)

    except pymongo.errors.ConnectionFailure as e:
        raise ConnectionError("Connection to MongoDB server failed")
    except pymongo.errors.PyMongoError as e:
        raise Exception("An error occurred during MongoDB operation")
    except json.decoder.JSONDecodeError as e:
        raise Exception(
            "Invalid JSON input: Check the query file format or the projection parameters must be enclosed in simple quotes. Example: '{\"...\"}'"
        )
    finally:
        # Ensure connection is closed even on errors
        if conn:
            conn.close()


# define the main call
if __name__ == "__main__":
    get_mongo_data(
        uri="mongodb://usr_prep_lectura:dkRp4A34@10.255.19.79:27012",
        database="traza_prep",
        collection="processed-data",
        query_file="query.json",
        projection="{}",
        limit=10,
    )
