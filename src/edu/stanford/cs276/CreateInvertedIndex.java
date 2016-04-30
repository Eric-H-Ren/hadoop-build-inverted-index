package edu.stanford.cs276;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class creates an inverted index using the Hadoop mapreduce framework.
 * Here's a sample command line.  It is invoked from the directory in which
 * Hadoop is installed.  It assumes that this program is exported as a jar
 * file into that directory.  The docs directory has the files to be indexed.
 * The output directory has the index.
 * 
 *   bin/hadoop jar HadoopExamples.jar \
 *     edu.stanford.cs276.CreateInvertedIndex \
 *     ../docs \
 *     ../output/
 *
 */
public class CreateInvertedIndex {
	
	/**
	 * The Map class takes files in TextInputFormat as input.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text term = new Text();
		private Text fileName = new Text();
		
		/**
		 * Defines the mapper for creating an inverted index.
		 * 
		 * @param key		Byte offset in the file where this line starts
		 * @param value 		The line of text from the file
		 * @param context	Context for collecting output key/value pairs.
		 * 					You can access all sorts of useful information with this.
		 */
		public void map(LongWritable key,
						Text value,
					 	Context context) throws IOException, InterruptedException {
			// Get the filename (the final component of the path) associated with the
			// file being read.
			// We make the simplifying assumption that the filename is a unique identifier
			// for the file.
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			fileName.set(fileSplit.getPath().getName());
			
			// 
			// 1. WRITE THE CORE LOGIC OF YOUR MAPPER HERE
			//    - the value argument to map(...) is a single line from 
			//      an input text file.  Tokenize lines by breaking on non-word
			//		characters (use split("\\W")).
			//    - make sure to normalize terms using String.toLowerCase()
			//    - call context.write(key, value) to output key/value pairs 
			//      from this mapper
			//

		}
	}
	
	/**
	 * The Reduce class creates an inverted index for each key.  It also
	 * converts each fileName to a docId and writes out the mapping using
	 * MultipleOutputs.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		// The following three member variables are initialized in setup() below.
		// Holds the mapping from filenames to docIds.
		private TreeMap<String, Integer> docIdDict;
		// Holds the next available docId
		private int nextDocId;
		// Multiple output named "dict" is used to output the dictionary.
		private MultipleOutputs<Text,Text> multipleOutputs;
	
		@Override
		protected void setup(Context context) {
			docIdDict = new TreeMap<String, Integer>();
			nextDocId = 1;
			multipleOutputs = new MultipleOutputs<Text,Text>(context);
		}
		
		@Override
		protected void cleanup(Context contex) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
		
		/**
		 * Returns the docId for the specified fileName.  If the fileName
		 * has already been assigned a docId, returns it.  Otherwise it
		 * assigns the next available docId to this fileName.  Also writes
		 * out any new docId/fileName mapping to the dict multiple output.
		 * 
		 * @param fileName	File name for which a docId is desired.
		 * @return			The docId assigned to fileName
		 */
		private Integer getDocId(String fileName) 
			throws IOException, InterruptedException {
			Integer docId = docIdDict.get(fileName);
			if (docId != null) {
				return docId;
			} else {  // create a new docId for this filename
				Integer newDocId = new Integer(nextDocId++);
				docIdDict.put(fileName, new Integer(newDocId));
				// Output the new entry in the dictionary into the
				// "dict" multiple output.
				multipleOutputs.write("dict", new Text(newDocId.toString()), new Text(fileName));
				return newDocId;
			}
		}
		
		/**
		 * Writes out the encoded inverted index for each key to context.  Assigns docIds
		 * to each fileName in values. Also writes out the fileName/docId mapping
		 * as a separate output.
		 * 
		 * @param key		Term for which index is being created
		 * @param values		The file names in which the term occurs.  Can
		 * 					have duplicate filenames
		 * @param context	Context for collecting output key/value pairs
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//
			// 2. WRITE THE CORE LOGIC OF YOUR REDUCER HERE
			//   - iterate through the values for this key using
			//     for (Text fileName : values) { }
			//   - use the getDocID(...) method (defined above) to convert
			//     a filename into a docId.
			//   - you can use TreeSet<Integer> to get the sorted, unique
			//     docIds
			//   - you can use encodePostingsList(...) (see below) to encode
			//     the postings list for a key
			//   - use context.write(key, value) to output from this reducer
			//
		}
		
		/**
		 * Encodes the set of docIds for a key.  This implementation
		 * simply converts into a space separated string of docIds.
		 * A real implementation would do gap encoding and compression.
		 * 
		 * @param docIds		Set of docIds for a key.
		 * @return			A Text object representing the encoding
		 */
		public Text encodePostingsList(TreeSet<Integer> docIds) {
			StringBuilder docIdStr = new StringBuilder();
			for (Integer docId: docIds) {
				if (docIdStr.length() > 0) {
					docIdStr.append(" ");
				}
				docIdStr.append(docId.intValue());
			}
			return new Text(docIdStr.toString());
		}
	}
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		/**
		 * Removes duplicate fileNames in values and writes the remaining fileNames 
		 * to context.
		 * 
		 * @param key		Term for which index is being created
		 * @param values		The file names in which the term occurs.  Can
		 * 					have duplicate filenames
		 * @param context	Context for collecting output key/value pairs
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			//
			// 3. WRITE THE CORE LOGIC OF YOUR COMBINER HERE
			//    Essentially, you want to remove duplicate fileNames from values
			//    and output the remaining <key, value> pairs.
			//

		}
	}
	
	public static class Partition extends Partitioner<Text, Text> {
		/**
		 * Partitions the key/value pairs based on value hash (rather than key hash).
		 * The idea is to partition the files so that all terms related to a file are
		 * in the same partition
		 * 
		 * @param key			The term in the file (ignored by this method)
		 * @param value			The fileName on which partitioning is done
		 * @param numPartitions	The number of partitions
		 * @return				partition number for this key/value pair.
		 * 						Value is from 0 to (numPartitions - 1).
		 */
		public int getPartition(Text key, Text value, int numPartitions) {
			//
			// 4. WRITE THE CORE LOGIC OF YOUR PARTITION FUNCTION HERE
			//  - Essentially you want your partition function to partition
			//    the documents (not the terms).
			//  - You can get a hash of any Object using the hashCode() method
			//    
			return 0;  // Remove this line when you implement this method
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CreateInvertedIndex");
	    
	    job.setJarByClass(CreateInvertedIndex.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Combine.class);
	    job.setReducerClass(Reduce.class);
	    job.setPartitionerClass(Partition.class);
	    
	    // using TextInputFormat means that each line in a text file is
	    // a record that is submitted to a mapper
		job.setInputFormatClass(TextInputFormat.class);
		// Output is a simple text file with each line consisting
		// of a term (the key) and and the encoded postings list (the value)
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// In addition to the postings list, we also output the mapping between
		// the docIds and corresponding fileNames
		MultipleOutputs.addNamedOutput(job, "dict", 
									  TextOutputFormat.class,
									  Text.class,
									  Text.class);
		
		// UNCOMMENT THE FOLLOWING LINE IF YOU WANT MORE THAN ONE REDUCER
		// job.setNumReduceTasks(2);
		
		// Specify the location of the input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
