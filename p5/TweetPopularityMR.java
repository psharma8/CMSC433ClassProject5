package cmsc433.p5;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 2;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper	extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable two = new IntWritable(2);
		private Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Tweet tweet = Tweet.createTweet(value.toString());
			
			if(trendingOn.equals(TrendingParameter.USER)) {
				word.set(tweet.getUserScreenName());
				context.write(word, one);
				if(tweet.wasRetweetOfUser()) {
					word.set(tweet.getRetweetedUser());
					context.write(word, two);
				}
				List<String> mentionedUsers = tweet.getMentionedUsers();
				for(String mentionuser : mentionedUsers) {
					word.set(mentionuser);
					context.write(word, one);
				}
			}else if(trendingOn.equals(TrendingParameter.TWEET)) {
				String id = tweet.getId().toString();
				word.set(id);
				context.write(word,one );
				if(tweet.wasRetweetOfTweet()) {
					String originalId = tweet.getRetweetedTweet().toString();
					word.set(originalId);
					context.write(word, two);
				}
			}else if(trendingOn.equals(TrendingParameter.HASHTAG)) {
				List<String>hashTags = tweet.getHashtags();
				for(String hashtag : hashTags) {
					word.set(hashtag);
					context.write(word, one);
				}
			}else if(trendingOn.equals(TrendingParameter.HASHTAG_PAIR)) {
				List<String> list = tweet.getHashtags();
				Collections.sort(list);
				if(list.size() >= 2) {
					for (int i = 0; i < list .size()-1; i++) {
						for(int j = i+1; j < list.size(); j++) {
							word.set("("+list.get(i)+","+list.get(j)+")");
							context.write(word, one);
						}
					}
				}
			}

		}
	}

	public static class PopularityReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);	
		
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
}
