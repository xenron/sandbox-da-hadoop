package com.packt.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetsCollector {
	private class ExampleListener implements StatusListener {
    	long count = 0;
    	static final long maxCount = 200000; // Number of tweets you want to collect.
    	PrintWriter out;
    	TwitterStream tweetStream;
    	
    	ExampleListener (TwitterStream ts) throws IOException {
    		tweetStream = ts;
    		out = new PrintWriter(new BufferedWriter(new FileWriter("\\user\\hue\\Tweets\\tweets.txt")));
    	}
    	
        public void onStatus(Status status) {
        	String username = status.getUser().getScreenName();
        	String text = status.getText().replace('\n', ' ');
            out.println(username + "\t" + text);
            System.out.println(username + "\t" + text);
            count++;
            if(count >= maxCount) {
            	tweetStream.shutdown();
            	out.close();
            }
        }

		@Override
		public void onDeletionNotice(StatusDeletionNotice arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onException(Exception arg0) {
			// TODO Auto-generated method stub
			
		}
	

		@Override
		public void onTrackLimitationNotice(int arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onScrubGeo(long arg0, long arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub
			
		}
	}
	
	StatusListener makeListener(TwitterStream ts) throws IOException {
		return this.new ExampleListener(ts);
	}
	
	public static void main(String[] args) throws IOException {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey("*********")
		  .setOAuthConsumerSecret("************")
		  .setOAuthAccessToken("*******************")
		  .setOAuthAccessTokenSecret("****************");
		TwitterStreamFactory tf= new TwitterStreamFactory(cb.build());
		TwitterStream twitterStream =tf.getInstance();
		TweetsCollector td = new TweetsCollector();
        StatusListener listener = td.makeListener(twitterStream);
        twitterStream.addListener(listener);
        twitterStream.sample();
	}

}
