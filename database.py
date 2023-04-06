import subprocess
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/")

# Define database and collection names
db_name = "db_mongo"
collection_name_1 = "comments"
collection_name_2 = "movies"
collection_name_3 = "theaters"
collection_name_4 = "users"

db_instance = myclient[db_name]
commentsCollection = db_instance[collection_name_1]
moviesCollection = db_instance[collection_name_2]
theatersCollection = db_instance[collection_name_3]
usersCollection = db_instance[collection_name_4]


class create_database_collections():
    def main():
        # Create database using MongoDB shell command
        subprocess.run(["mongo", "--eval", f"db.createDatabase('{db_name}')"])
        
        # Create collection using MongoDB shell command
        subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_1}')", db_name])
        subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_2}')", db_name])
        subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_3}')", db_name])
        subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_4}')", db_name])
        
        # Define path to JSON file containing data to import
        json_file_path = "/Users/sonali_gudey/Downloads/sample_mflix/comments.json"
        
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name_1, "--file", json_file_path])
        json_file_path = "/Users/sonali_gudey/Downloads/sample_mflix/movies.json"
        
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name_2, "--file", json_file_path])
        json_file_path = "/Users/sonali_gudey/Downloads/sample_mflix/theaters.json"
        
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name_3, "--file", json_file_path])
        json_file_path = "/Users/sonali_gudey/Downloads/sample_mflix/users.json"
        
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name_4, "--file", json_file_path])
        
    if __name__ == "__main__":
        main()
        
