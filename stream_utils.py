import json


def read_database_credentials(filename):
    with open(filename, "r") as file:
        return json.load(file)


def read_keywords(filename):
    with open(filename, "r") as file:
        keywords = [x.strip() for x in file.readlines()]
    return keywords
