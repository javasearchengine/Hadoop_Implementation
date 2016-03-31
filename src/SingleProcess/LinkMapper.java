package SingleProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 	
 	
 	   public class LinkMapper extends MapReduceBase 
 	   					implements Mapper<LongWritable, Text, Text, IntWritable> {
 	     
 		   private final static IntWritable one = new IntWritable(1);
 		   private Text link = new Text();
 	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, 
 	    		 		Reporter reporter) throws IOException {
 	       String line = value.toString();
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       while (tokenizer.hasMoreTokens()) 
 	       {
 	         link.set(tokenizer.nextToken());
 	         output.collect(link, one);
 	       }
 	     }
 	   }
 	