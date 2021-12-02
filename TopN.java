import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class TopN {	
	public static class TopNMapper
	extends Mapper<Object, Text, Text, IntWritable> {
		private HashMap<String, Integer> term_freq;
		private Text word = new Text();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			term_freq = new HashMap<String, Integer>();
		}
		
		public void map(Object term, Text value, Context context)
		throws IOException, InterruptedException {
			String[] stopwords_array = {"he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or", "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "you're", "you've", "you'll", "you'd", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers", "herself", "it", "it's", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "that'll", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "should've", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't", "hadn", "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't", "mustn", "mustn't", "needn", "needn't", "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't", "weren", "weren't", "won", "won't", "wouldn", "wouldn't", "around", "one", "every"};
			HashSet<String> stopwords = new HashSet<String>(Arrays.asList(stopwords_array));
			
			// Keep a word count in the HashMap
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String word_string = word.toString().toLowerCase();
				if(stopwords.contains(word_string)){
					continue;
				}
				if(term_freq.containsKey(word_string)) {
					term_freq.replace( word_string, term_freq.get(word_string) + 1);
				} else {
					term_freq.put(word_string, 1);
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			// Add all values to an ArrayList for sorting
			ArrayList<Entry<String, Integer>> term_freqs = new ArrayList<Entry<String, Integer>>();
			for (HashMap.Entry<String, Integer> term_f: term_freq.entrySet()) {
				term_freqs.add(new java.util.AbstractMap.SimpleEntry<>(term_f.getKey(), term_f.getValue()));
			}
			
			// Get N from the command line arguments
			Configuration conf = context.getConfiguration();
			Integer N = Integer.parseInt(conf.get("N"));

			// Sort all the values received by the mapper
			Collections.sort(term_freqs, new Comparator<Map.Entry<String, Integer> >() {
				public int compare(Map.Entry<String, Integer> o1,
								   Map.Entry<String, Integer> o2)
				{
					return (o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			// Output the Top N results (or all if terms < N)
			int i = 0;
			for (Entry<String, Integer> term_f: term_freqs) {
				context.write(new Text(term_f.getKey()), new IntWritable(term_f.getValue()));
				i++;
				if(i >= N) {
					break;
				}
			}
		}
	}
	
	public static class TopNReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private HashMap<String, Integer> term_freq;
		
		// Since we can't determine top-n until we have seen all values, 
		//  create an array to store values until we can sort them at the end
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			term_freq = new HashMap<String, Integer>();
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {

			// Keep a word frequency for all terms in a HashMap
			for(IntWritable freq : values) {
				if(term_freq.containsKey(key.toString())) {
					term_freq.replace( key.toString(), term_freq.get(key.toString()) + freq.get());
				} else {
					term_freq.put(key.toString(), freq.get());
				}
			}
		}
	
		// After we've received every value, then top-n can be determined
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			// Add all values to an ArrayList for sorting
			ArrayList<Entry<String, Integer>> term_freqs = new ArrayList<Entry<String, Integer>>();
			for (HashMap.Entry<String, Integer> term_f: term_freq.entrySet()) {
				term_freqs.add(new java.util.AbstractMap.SimpleEntry<>(term_f.getKey(), term_f.getValue()));
			}
			
			// Get N from the command line arguments
			Configuration conf = context.getConfiguration();
			Integer N = Integer.parseInt(conf.get("N"));
			
			// Sort all the values received by the reducer
			Collections.sort(term_freqs, new Comparator<Map.Entry<String, Integer> >() {
				public int compare(Map.Entry<String, Integer> o1,
								   Map.Entry<String, Integer> o2)
				{
					return (o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			// Output the Top N results (or all if terms < N)
			int i = 0;
			for (Entry<String, Integer> term_f: term_freqs) {
				context.write(new Text(term_f.getKey()), new IntWritable(term_f.getValue()));
				i++;
				if(i >= N) {
					break;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		// Ensure arguments are valid
		if (args.length != 3) {
			System.err.println("Usage: TopN <input path> <output path> <N>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		// Pass the N argument to the MapReduce functions
		conf.set("N", args[2]);
		
		Job job = Job.getInstance(conf, "Top N");
		job.setJarByClass(TopN.class);
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Only use 1 reducer for Top N
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
