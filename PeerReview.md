## Peer Learning Review

### Ashish's approach:

comments_collection_analysis:
1. The init function initializes the Comments object and sets the "collection" attribute to the "comments" collection from a MongoDB database.
2. The insert_document function prompts the user to input data in JSON format, converts it to a dictionary, and inserts it into the "comments" collection using the insert_one method.
3. In the users_with_max_comments method, he used the MongoDB aggregation pipeline and grouped the comments by user name using $group, counted the number of comments per user, sorted the result in descending order by count, which in turn returns the top 10 users with maximum comments.
4. The movies_with_max_comments method grouped the comments by movie ID, counted the number of comments per movie, sorted the result in descending order by count, and returned the top 10 movies with maximum comments.
5. The comments_per_month method, first filtered the comments by year using $match, then grouped them by month, counted the number of comments per month, and returned the result sorted by month.

movies_collection_analysis:
1. The movies_with_highest_rating method first, filters out any movies with no IMDb rating using $match, then it sorts the remaining movies in descending order by their IMDb rating, then it projects only the movie titles and ratings using $project, and finally it limits the result to the top N movies.
2. The movies_with_highest_rating_in_given_year method is similar to movies_with_highest_rating, but it adds an additional filter to only consider movies released in a specific year using $eq.
3. The movies_with_highest_rating_with_given_votes method is similar to movies_with_highest_rating, but it filters out movies with fewer than 1000 IMDb votes using $gt.
4. The movies_with_highest_rating_with_given_pattern method is similar to movies_with_highest_rating, but it uses a regular expression pattern to filter the movies by title instead of using a year or vote count using $regex.
5. The directors_with_max_movies method uses the $unwind stage to split the directors array into separate documents for each director, then grouping the resulting documents by director and using the $sum to count the number of movies each director has made, and finally sorting the results by the count of movies and limiting the output to the top N directors.
6. The directors_with_max_movies_in_given_year method is similar to directors_with_max_movies, but it adds an additional filter to only consider movies released in a specific year.
7. The directors_with_max_movies_with_genre method is similar to directors_with_max_movies, but it adds an additional filter to only consider movies with a specific genre.
8. The actors_with_max_movies method is similar to directors_with_max_movies, but it counts the number of movies each actor has appeared in instead of each director.
9. The actors_with_max_movies_in_given_year method is similar to actors_with_max_movies, but it adds an additional filter to only consider movies released in a specific year.

theatre_collection_analysis:
1. The cities_with_max_theaters() method groups the theaters by their city using the $group aggregation stage. Then, it calculates the count of theaters in each city using the $sum aggregation operator. After that, it sorts the result in descending order of theater count using the $sort aggregation stage. Finally, it limits the result to the top 10 using the $limit aggregation stage.
2. The theaters_nearby_given_coordinate(coords, max_dist) retrieves all the documents from the "theaters" collection and stores the theater IDs and their coordinates in a list of dictionaries using the $project aggregation stage. Then, it calculates the distance between the target coordinates and the coordinates of each theater using the Pythagorean theorem. If the distance is less than or equal to the maximum distance provided, the theater ID is added to a list along with its distance from the target coordinates.


### Aakash's approach:

comments:
1. top 10 users who made the maximum number of comments, grouped the document by email id (unique identifier), counted the number of comments for each email id using sum, sorted the result by number of comments in decreasing order, taken the top 10 results using limit.
2. top 10 movies with most comments, grouped the document by movie id, counted the number of comments for each movie id, sorted the result by number of comments in decreasing order, taken the top 10 results using limit.
3. total number of comments created in each month in a given year, projected the year and month of each document, taken the document which matches the year entered by user, grouped the document by the month, counted the number of comments for each month using sum, sorted the result by month (jan-dec).

movies:
1. top N movies by highest IMDB rating, taken the documents where IMDB rating is not null, projected the movie title and it's IMDB rating, sorted the result by IMDB rating in decreasing order and then by title name in ascending order, taken the top n results using limit.
2. top N movies with the highest IMDB rating in given year, projected the movie title, it's IMDB rating and it's year of release, taken the documents where IMDB rating is not null and year of release matches the year entered by user, sorted the result by IMDB rating in decreasing order and then by title name in ascending order, taken the top n results using limit.
3. top N movies with the highest IMDB rating with votes>1000, taken the documents where IMDB rating is not null and votes>1000, projected the movie title and it's IMDB rating, sorted the result by IMDB rating in decreasing order and then by title name in ascending order, taken the top n results using limit.
4. top N movies with title matching a given pattern sorted by highest tomatoes ratings, taken the documents where movie title matches a given pattern using regex expression, projected the movie title and it's Tomato rating, sorted the result by Tomato rating in decreasing order and then by title name in ascending order, taken the top n results using limit.
5. top N directors who created the maximum number of movies, splited the directors name array into individual document using unwind, grouped the document by director name, counted the number of directors for each movie using sum, sorted the result by number of directors in decreasing order, taken the top n results using limit.
6. top N directors who created the maximum number of movies in a given year, taken the documents where year relaese matches the year entered by user, splited the directors name array into individual document using unwind, grouped the document by director name, counted the number of directors for each movie using sum, sorted the result by number of directors in decreasing order, taken the top n results using limit.
7. top N directors who created the maximum number of movies for a given genre, taken the documents where movie genre matches the genre entered by user, splited the directors name array into individual document using unwind, grouped the document by director name, counted the number of directors for each movie using sum, sorted the result by number of directors in decreasing order, taken the top n results using limit.
8. top N actors who starred in the maximum number of movies, splited the actors name array into individual document using unwind, grouped the document by actors name, counted the number of actors for each movie using sum, sorted the result by number of actors in decreasing order, taken the top n results using limit.
9. top N actors who starred in the maximum number of movies in a given year, taken the documents where year relaese matches the year entered by user, splited the actors name array into individual document using unwind, grouped the document by actors name, counted the number of actors for each movie using sum, sorted the result by number of actors in decreasing order, taken the top n results using limit.
10. top N actors who starred in the maximum number of movies for a given genre, taken the documents where movie genre matches the genre entered by user, splited the actors name array into individual document using unwind, grouped the document by actors name, counted the number of actors for each movie using sum, sorted the result by number of actors in decreasing order, taken the top n results using limit.
11. top N movies for each genre with the highest IMDB rating, splited the movies genre array into individual document using unwind, grouped the document by movie genre, stored the result of above query in a variable result, looped through the result and taken the documents where IMDB rating is not null and with each genre, projected the movie title and IMDB rating, sorted the result by number of comments in decreasing order, taken the top 10 results using limit.

theatres:
1. top 10 cities with the maximum theaters, grouped the document by city, counted the number of theaters for each city using sum, sorted the result by number of theaters in decreasing order, taken the top 10 results using limit.
2. top 10 theatres nearby given coordinates, used the geo spatial functions for finding the top 10 theaters nearby with the coordinates entered by users, taken the top 10 results using limit.
