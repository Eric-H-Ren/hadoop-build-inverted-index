package edu.stanford.cs276;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CreateLM {
	
	/** UNCOMMENT THIS WHEN YOU FIX UP THIS METHOD
	//
	// 1. WHAT TYPES SHOULD BE USED TO SPECIALIZE Mapper?
	//    Fill in the question marks with appropriate types
	//
	public static class Map extends Mapper<?, ?, ?, ?> {
		
		private static Text totalUnigramsTerm = new Text("__totalUnigramsTerm__");
		private static Text totalBigramsTerm = new Text("__totalBigramsTerm__");
	
		//
		// 2. FILL IN THE TYPES OF KEY AND VALUE
		//
		@Override
		public void map(? key, ? value, Context context)
			throws IOException, InterruptedException {
	
			//
			// 7. WRITE THE LOGIC FOR YOUR MAPPER HERE
			//
			
		}
	}
	**/
	
	/** UNCOMMENT THIS WHEN YOU FIX THIS METHOD
	//
	// 3. WHAT TYPES SHOULD BE USED TO SPECIALIZE Reducer?
	//    Fill in the question marks with appropriate types
	//
	public static class Reduce extends Reducer<?, ?, ?, ?> {

		//
		// 4. FILL IN THE TYPES OF KEY AND VALUE
		//
		@Override
		public void reduce(? key, ? values, Context context)
				throws IOException, InterruptedException {

			//
			// 8. WRITE THE LOGIC FOR YOUR REDUCER HERE
			//

		}
	}
	**/
	
	// WholeFileTextInputFormat is exactly like TextInputFormat except that
	// it isn't splitable.  This means that the whole file is presented as the
	// value to the mapper (not just a line).
	public static class WholeFileTextInputFormat extends TextInputFormat {
		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			return false;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CreateLM");
		
	    job.setJarByClass(CreateLM.class);
	    
	    //
	    // 5. SETUP MAPPER, REDUCER, COMBINER
	    //    What class will you use for the combiner?
	    //
	    
	    //
	    // 6. SETUP THE InputFormatClass.
	    //    Why does TextInputFormat not work?
	    //
	    
		// Output is a simple text file with each line consisting
		// of a unigram or bigram term (the key) and its count (the value)
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
