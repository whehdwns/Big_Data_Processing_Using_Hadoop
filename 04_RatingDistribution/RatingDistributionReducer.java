/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package bdpuh.assignment4;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hdadmin
 */
public class RatingDistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
        i = 0;
        for (IntWritable val: values) {
            
            i += val.get();
        }
        count.set(i);
        context.write (key, count);
    }
}
