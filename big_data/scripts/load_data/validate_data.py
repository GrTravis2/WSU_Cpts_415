"""Travis's validation script."""

# iterate data folder and reading stat text files

# write mongo db queries to pull down data and compare to the stat text files

# check if it matches

import io
from contextlib import redirect_stdout
from pathlib import Path

from pymongo import MongoClient
from scripts.load_data.load import _iter_data

# check rows parsed vs rows in mongoDB


def count_docs(collection):
    """Count total documents."""
    # count the documents in MongoDB
    total_documents = collection.count_documents({})
    print("Total documents in MongoDB:", total_documents)

    # count documents in raw data
    total_raw_documents = 0
    total_parse_fails = 0
    raw_file = Path("data/raw")

    buffer = io.StringIO()
    with redirect_stdout(buffer):  # temporarily catch all prints
        if not (raw_file.exists()):
            print("Could not find raw data file!!")
        else:
            for _, data, _ in _iter_data(raw_file):
                total_raw_documents += len(data)

    # look through buffer to find "failed to parse" lines
    total_parse_fails = sum(1 for line in buffer.getvalue().splitlines() if "failed to parse" in line)

    print("Total Documents Parsed:", total_raw_documents)
    print("Total Failed Parses:", total_parse_fails)


def validate_schema(collection):
    """Check for missing fields."""
    # check for missing fields
    bad_docs = 0
    doc_scanner = collection.find({}, {"_id": 0}).limit(10000)

    for doc in doc_scanner:
        # check required keys
        required_fields = [
            "id",
            "uploader_name",
            "age_days",
            "category",
            "length_seconds",
            "views",
            "video_rating",
            "num_ratings",
            "num_comments",
            "related_ids",
        ]
        for field in required_fields:
            if field not in doc:
                print("Missing field:", field)
                bad_docs += 1
    print("Documents missing fields:", bad_docs)


def detect_duplicates(collection):
    """Find duplicate entries."""
    pipeline = [
        {"$group": {"_id": "$id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]

    duplicate_entries = collection.aggregate(pipeline)

    print("Duplicate ID groups:")
    for entry in duplicate_entries:
        print(f"ID: {entry['_id']} appears {entry['count']} times")


def main():
    """Script entry point."""
    print("Starting MongoDB data validation...")
    # connect to mongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_analysis"]
    collection = db["videos"]

    count_docs(collection)
    validate_schema(collection)
    detect_duplicates(collection)

    # close mongoDB
    client.close()


# run main
main()
