package bdpuh.mapreduceintermediate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    int i = 0;
    IntWritable count = new IntWritable();

    @Override
    public int getPartition(Text key, IntWritable value, int numReducers) {
        // We know numReducers is 2, so simplifying code
        String strKey = key.toString();
        if (strKey.charAt(0) <= 'M') {
            return 0;
        }
        else {
            return 1;
        }
    }
    
    
       
}
