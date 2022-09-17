ratings = LOAD '/pig/u.data' as (user_id:int, item_id:int, rating:int, rating_time:chararray);

item = LOAD '/pig/u.item' using PigStorage('|') as (movie_id:int, movie_title:chararray, release_date:chararray, video_release_date:chararray, IMDB_URL:chararray, unknown:int,  Action:int,  Adventure:int,  Animation:int,  Children:int,  Comedy:int,  Crime:int,  Documentry:int,  Drama:int,  Fantasy:int,  Film_Noir:int, Horror:int,  Musical:int,  Mystery:int,  Romance:int,  Sci_Fi:int,  Thriller:int, War:int, Western:int);

ratingsByMovie = GROUP ratings BY item_id;

averageRatings = FOREACH ratingsByMovie GENERATE group AS item_id, AVG(ratings.rating) as avgRating;

movielist = JOIN averageRatings BY item_id, item BY movie_id;

movieresult = FOREACH movielist GENERATE movie_id, movie_title, release_date, IMDB_URL, avgRating;

store movieresult INTO '/out';
