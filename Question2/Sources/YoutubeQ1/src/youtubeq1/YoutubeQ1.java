package youtubeq1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YoutubeQ1 {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {		
		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");
			if(row.length >= 4) {
				context.write(new Text(row[3]), new IntWritable(1));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private ArrayList<String[]> topFiveCatagories = 
				new ArrayList<String[]>();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {				
			int sum = 0;
			for(IntWritable value : values) {
				sum += Integer.parseInt(value.toString());
			}
			
			if(topFiveCatagories.size() < 5) {
				addToArrayList(topFiveCatagories, key.toString(), sum);
			} else if(Integer.parseInt(topFiveCatagories.get(4)[1]) < sum) {
				topFiveCatagories.remove(4);
				addToArrayList(topFiveCatagories, key.toString(), sum);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(String[] cat : topFiveCatagories) {
				context.write(new Text(cat[0]), new IntWritable(Integer.parseInt(cat[1])));
			}
		}
		
		private void addToArrayList(ArrayList<String[]> array, 
				String key, int value) {
			int index = 0;
			while(index < array.size() && 
				Integer.parseInt(array.get(index)[1])
				> value) {
				++index;
			}
			String[] video = {key, String.valueOf(value)};
			array.add(index, video);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
            System.err.println("Usage: YoutubeQ1 <in_dir> <out_dir>");
            System.exit(2);
        }
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "YoutubeQ1");
		job.setJarByClass(YoutubeQ1.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
