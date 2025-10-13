"""load to mongodb."""

from __future__ import annotations

import argparse
import traceback
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure
from scripts.load_data.data_types import YouTubeData
from scripts.load_data.load import _iter_data


class MongoDBLoader:
    """loader class."""

    # automatically create the db and connection
    # change uri if needed
    def __init__(
        self,
        connection_uri: str = "mongodb://localhost:27017/",
        db_name: str = "youtube_analysis",
    ):
        """Construct db hookup."""
        self.client = MongoClient(connection_uri)
        self.db = self.client[db_name]
        self.collection = self.db["videos"]

    # pingtest
    def test_connection(self) -> bool:
        """Pingtester."""
        try:
            self.client.admin.command("ping")
            print("Successfully connected to MongoDB")
            return True
        except ConnectionFailure:
            print("Failed to connect to MongoDB")
            return False

    def close(self) -> None:
        """Close db conn."""
        self.client.close()
        print("MongoDB connection closed")

    def create_indexes(self) -> None:
        """Lookup indexes."""
        # fast lookup indexes, kinda cool
        self.collection.create_index(
            [("upload_date", 1), ("id", 1)],
            unique=True,
            name="composite_primary_key",
        )
        self.collection.create_index("id", name="video_id")
        self.collection.create_index("video_desc.uploader", name="uploader_index")
        self.collection.create_index("video_desc.category", name="category_index")
        self.collection.create_index("video_engagement.views", name="views_index")
        self.collection.create_index("video_attri.rating", name="rating_index")
        print("Database indexes created")

    # read the date from the directory, 4 and 6 digit versions
    def parse_directory_date(self, dir_name: str) -> Optional[datetime]:
        """Date parser."""
        try:
            if len(dir_name) == 4 and dir_name.isdigit():
                # if 4 digit year is 2007
                month = int(dir_name[:2])
                day = int(dir_name[2:])
                return datetime(2007, month, day)
            elif len(dir_name) == 6 and dir_name.isdigit():
                # 6 digit
                year = int(dir_name[:2])
                month = int(dir_name[2:4])
                day = int(dir_name[4:6])
                # convert to full years 4 digit
                full_year = 2000 + year if year < 100 else year
                return datetime(full_year, month, day)
            else:
                print(f"Warning: Could not parse dir name as date:{dir_name}")
                return None
        except ValueError as e:
            print(f"Error parsing date from directory '{dir_name}': {e}")
            return None

    # simplify data for mongo
    def transform_data(
        self,
        data: YouTubeData,
        upload_date: Optional[datetime] = None,
    ) -> dict:
        """Prepare data for mongo."""
        document = {
            "id": data.id,
            "video_desc": {
                "uploader": data.uploader_un,
                "age_days": data.age_days,
                "category": data.category,
            },
            "video_attri": {"length": data.length_s, "rating": data.video_rate},
            "video_engagement": {
                "views": data.views,
                "num_ratings": data.num_ratings,
                "num_comments": data.num_comments,
            },
            "related_ids": data.related_ids,
        }
        # if upload date was successfully parsed
        if upload_date:
            document["upload_date"] = upload_date

        return document

    def load_data(
        self,
        data_generator: Generator[tuple[str, list[YouTubeData]]],
    ) -> dict:
        """Load data into mongodb."""
        # stats
        total_processed = 0
        total_inserted = 0
        total_errors = 0
        total_duplicates = 0

        # loop through data from load
        for dir_name, data_list in data_generator:
            # date parsing
            upload_date = self.parse_directory_date(dir_name)
            date_info = (
                f" (date: \
                    {upload_date.strftime('%Y-%m-%d')}\
                        )"
                if upload_date
                else ""
            )
            print(
                f"Processing {len(data_list)}\
                  records from {dir_name}{date_info}"
            )

            # more stats
            dir_records = len(data_list)
            total_processed += dir_records
            transformation_errors = 0

            # convert and add to document list
            documents = []
            for data in data_list:
                try:
                    document = self.transform_data(data, upload_date)
                    documents.append(document)
                except Exception as e:
                    print(f"Error transforming data: {e}")
                    transformation_errors += 1
                    continue

            total_errors += transformation_errors

            # if was good input insert doc list and check for dupes
            if documents:
                try:
                    # has to be short because the linter
                    # is very stupid
                    # stands for result
                    r = self.collection.insert_many(documents, ordered=False)
                    inserted_count = len(r.inserted_ids)
                    total_inserted += inserted_count
                    print(
                        f"Inserted {inserted_count} documents \
                            from {dir_name}{date_info}"
                    )

                    duplicates = len(documents) - inserted_count
                    total_duplicates += duplicates

                    if duplicates > 0:
                        print(
                            f"Successfully inserted {inserted_count} \
                                documents, skipped {duplicates} \
                                    duplicates from {dir_name}{date_info}"
                        )
                    else:
                        print(
                            f"Successfully inserted {inserted_count}\
                                  documents from {dir_name}{date_info}"
                        )
                # if error, like if trying to insert unique dupe
                except BulkWriteError as e:
                    # get insertion stats even if failed
                    inserted_count = e.details["nInserted"]
                    total_inserted += inserted_count

                    real_errors = 0
                    for error in e.details["writeErrors"]:
                        if error.get("code") != 11000:
                            # duplicate key error in mongo
                            # will happen a lot so point out other errors
                            real_errors += 1

                    duplicates = len(documents) - inserted_count - real_errors
                    total_duplicates += duplicates
                    total_errors += real_errors

                    if real_errors > 0:
                        print(
                            f"Bulk write completed with\
                                  {real_errors} real errors\
                                  and {duplicates} duplicates \
                                    from {dir_name}{date_info}"
                        )
                    else:
                        print(
                            f"Bulk write completed with\
                                  {duplicates} duplicates \
                                from {dir_name}{date_info}"
                        )

                except Exception as e:
                    print(f"Error inserting batch: {e}")
                    total_errors += len(documents)

        # return the stats
        return {
            "total_processed": total_processed,
            "total_inserted": total_inserted,
            "total_errors": total_errors,
            "collection_count": self.collection.count_documents({}),
        }


def main() -> None:
    """Arg parsing, calls, and stats."""
    # use a argparse lib to read cli
    parser = argparse.ArgumentParser(
        prog="MongoDB Loader",
        description="Loads parsed YouTube data into MongoDB",
    )

    parser.add_argument(
        "--dir-path",
        type=str,
        help="Path to directory containing data files",
        required=True,
    )

    parser.add_argument(
        "--mongo-uri",
        type=str,
        help="MongoDB connection URI",
        default="mongodb://localhost:27017/",
    )

    parser.add_argument(
        "--db-name",
        type=str,
        help="MongoDB database name",
        default="youtube_analysis",
    )

    parser.add_argument(
        "--collection-name",
        type=str,
        help="MongoDB collection name",
        default="videos",
    )

    args = parser.parse_args()

    # Convert to Path object and validate
    dir_path = Path(args.dir_path)

    if not dir_path.exists():
        print(
            f"Error: Directory path \
              does not exist: {dir_path}"
        )
        return

    if not dir_path.is_dir():
        print(f"Error: Path is not a directory: {dir_path}")
        return
    # Initialize loader
    loader = MongoDBLoader(args.mongo_uri, args.db_name)

    # Test connection
    if not loader.test_connection():
        return

    # Create indexes
    loader.create_indexes()

    try:
        # parse files and add to mongo
        data_generator = _iter_data(dir_path)
        stats = loader.load_data(data_generator)

        # TODO fix total_processed overcounting due to double running
        print("\n" + "=" * 50)
        print("LOADING SUMMARY")
        print("=" * 50)
        print(
            f"Total records processed:\
               {stats['total_processed']}"
        )
        print(
            f"Total records inserted:\
               {stats['total_inserted']}"
        )
        print(
            f"Total duplicates skipped: \
              {stats.get('total_duplicates', 0)}"
        )
        print(
            f"Total errors: \
              {stats['total_errors']}"
        )
        print(
            f"Total in collection: \
              {stats['collection_count']}"
        )
        print("=" * 50)

    except Exception as e:
        print(f"Error during data loading: {e}")
        traceback.print_exc()
    finally:
        loader.close()
