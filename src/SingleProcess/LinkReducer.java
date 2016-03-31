package SingleProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class LinkReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {


		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			Document doc=null;
			String url=values.toString();
			try
			{
			doc = Jsoup.connect(url).timeout(10*1000).get(); 
			}
			catch(Throwable e)
			{
				System.out.println("Check your Internet connection and tray again!!!");
			}
			ArrayList<String> map = new ArrayList<String>();
			Elements links = doc.select("a[href]");
			
	        for (Element link : links) 
	        {
	           map.add(link.attr("abs:href"));
	         
	        }
	       int val=Integer.parseInt(map.toString());
	        output.collect(key, new IntWritable(val));
		}
	}