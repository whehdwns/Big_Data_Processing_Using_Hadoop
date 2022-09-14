/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package bdpuh.assignment5;

/**
 *
 * @author hdadmin
 */
public class MovieRatingsParameter {
    private Integer moviekey;
    private String movietitle; 
    private String releaseYear;
    private String imdb_web_url;
    private Integer unique_users;
    private Integer movie_ratings;
    private Integer movie_ratings_Average;
    
    public MovieRatingsParameter(){};
    
    public MovieRatingsParameter(int id, String title, String Year, String imdb, int users, int ratings, int ratingsAverage){
        this.moviekey = id;
        this.movietitle = title;
        this.releaseYear = Year;
        this.imdb_web_url = imdb;
        this.unique_users = users;
        this.movie_ratings = ratings;
        this.movie_ratings_Average = ratingsAverage;    
    }
    
    public int getId() { 
        return this.moviekey; 
    }
    
    public void setId(int id) { 
        this.moviekey = id; 
    }

    public String getTitle() {
        return movietitle;
    }

    public void setTitle(String movietitle) {
        this.movietitle = movietitle;
    }

    public String getYear() {
        return releaseYear;
    }

    public void setYear(String releaseYear) {
        this.releaseYear = releaseYear;
    }

    public String getimdb() {
        return imdb_web_url;
    }

    public void setimdb(String imdb_web_url) {
        this.imdb_web_url = imdb_web_url;
    }

    public int getusers() {
        return unique_users;
    }

    public void setusers(int unique_users) {
        this.unique_users = unique_users;
    }

    public int getRatings() {
        return movie_ratings;
    }

    public void setRatings(int movie_ratings) {
        this.movie_ratings = movie_ratings;
    }

    public int getRatingsAverage() {
        return movie_ratings_Average;
    }

    public void setRatingsAverage(int movie_ratingsSum) {
        this.movie_ratings_Average = movie_ratingsSum;
    }
    
    @Override
    public String toString() {
        String t = "\t";
        StringBuilder stringBuilder = new StringBuilder();
        
        stringBuilder.append(this.moviekey.toString()).append(t).append(this.movietitle).append(t)
              .append(this.releaseYear).append(t).append(this.imdb_web_url).append(t)
              .append(this.unique_users.toString()).append(t).append(this.movie_ratings.toString()).append(t)
              .append(this.movie_ratings_Average.toString());
        return stringBuilder.toString();
    }
}
