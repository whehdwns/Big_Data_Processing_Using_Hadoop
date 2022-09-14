
package bdpuh.assignment4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
/**
 *
 * @author hdadmin
 */

public class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    Logger logger = Logger.getLogger(RatingDistributionMapper.class);
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t+");
        word.set(split[2]);
        one.set(Integer.parseInt(split[2]));
        context.write(word, one);
        
    }   
}
