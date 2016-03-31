package SingleProcess;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
 
public class LinkJob  {
 
	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(LinkMapper.class);
	     conf.setJobName("LinkAnalyzer");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(LinkMapper.class);
	     conf.setCombinerClass(LinkReducer.class);
	     conf.setReducerClass(LinkReducer.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path("link.txt"));
	     FileOutputFormat.setOutputPath(conf, new Path("Analyzedlink.txt"));
	
	     JobClient.runJob(conf);
	}
	
	}