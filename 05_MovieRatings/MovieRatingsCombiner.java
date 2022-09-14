///*
// * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
// * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
// */
package bdpuh.assignment5;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author hdadmin
 */
public class MovieRatingsCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
    int i= 0;
    Text field = new Text();
    MovieRatingsParameter movieRow = new MovieRatingsParameter();
    
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        int i = 0;
        int sumrating = 0;
        for(Text val : values) {   //IntWritable should be Text(and above)
            String line = val.toString();
            String[] split =line.split("\\|");
            
            if(null == split[0]) {
                movieRow.setTitle("Record type is incomplete.");
            }
            else switch (split[0]) {
                case "D":
                    // u.data.record
                    i +=1;
                    sumrating += Integer.parseInt(split[2]);
                    break;
                case "I":
                    // u.iterm record
                    try{
                        movieRow.setTitle(split[1]);
                    }catch(ArrayIndexOutOfBoundsException ex){
                        movieRow.setTitle("No Title");
                    } try{
                        movieRow.setYear(split[2]);
                    }catch(ArrayIndexOutOfBoundsException ex){
                        movieRow.setTitle("No Release Year");
                    } try{
                        movieRow.setimdb(split[3]);
                    }catch(ArrayIndexOutOfBoundsException ex){
                        movieRow.setTitle("No IMDB URL");
                    } Counter counter = context.getCounter(MovieRatingsCounter.TOTAL_UNIQUE_MOVIES);
                    counter.increment(1);
                    break;
                default:
                    movieRow.setTitle("Record type is incomplete.");
                    break;
            }
        }
        double avg = (double)sumrating/i;
        movieRow.setusers(i);
        movieRow.setRatings(i);
        movieRow.setRatingsAverage((int) avg);
         
        field.set(movieRow.toString());
        context.write(key, field); 
    }       
}