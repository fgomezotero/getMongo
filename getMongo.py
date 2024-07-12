#!/usr/bin/env python3

# Implement an script getMongo to replace Nifi processor GetMongo
import pymongo, click, json


@click.command
@click.option("--host", "-h", help="MongoDB host")
@click.option("--port", "-p", default=27017, type=int, help="MongoDB port")
@click.option("--database", "-d", help="MongoDB database")
@click.option("--collection", "-c", help="MongoDB collection")
@click.option("--query", "-q", help="MongoDB query")
@click.option("--projection", "-j", help="MongoDB projection")
@click.option("--limit", "-l", default=0, type=int, help="Limit the number of results")
@click.option("--username", "-u", help="MongoDB username")
@click.option("--password", "-s", help="MongoDB password")
@click.option(
    "--authentication_database",
    "-a",
    help="MongoDB authentication database",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def get_mongo_data(
    host: str,
    port: int,
    database: str,
    collection: str,
    query: dict,
    projection: dict[str:int],
    limit: int = 0,
    username: str = None,
    password: str = None,
    authentication_database: str = None,
    verbose: bool = True,
) -> list:
    """_summary_

    Args:
        host (str): MongoDB host
        port (int): MongoDB post
        database (str): MongoDB database
        collection (str): MongoDB collection
        query (dict): MongoDB query
        projection (dict): MongoDB projection
        limit (int, optional): Limit the number of results. Defaults to 0.
        username (str, optional): MongoDB username. Defaults to None.
        password (str, optional): MongoDB password. Defaults to None.
        authentication_database (str, optional): MongoDB authentication database. Defaults to None.
        verbose (bool, optional): Verbose output. Defaults to True.

    Raises:
        ConnectionError: _description_
        Exception: _description_

    Returns:
        list: _description_
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

        query = json.loads(query)
        projection = json.loads(projection) if projection else None
        # Execute the query
        cursor = coll.find(query, projection)

        # Apply the limit if provided
        if limit > 0:
            cursor = cursor.limit(limit)

        # Convert the cursor to a list of dictionaries
        result = list(cursor)

        # Print the result in json format
        if verbose:
            click.echo(json.dumps(result, indent=2))
        else:
            click.echo(result)

    except pymongo.errors.ConnectionFailure as e:
        raise ConnectionError(f"Connection to MongoDB server failed: {e}") from e
    except pymongo.errors.PyMongoError as e:
        raise Exception(f"An error occurred during MongoDB operation: {e}") from e
    except json.decoder.JSONDecodeError as e:
        raise Exception(
            "Invalid JSON input: The query and projection parameters must be enclosed in simple quotes. Example: '{\"...\"}'"
        ) from e
    finally:
        # Ensure connection is closed even on errors
        if conn:
            conn.close()


# define the main call
if __name__ == "__main__":
    get_mongo_data()
