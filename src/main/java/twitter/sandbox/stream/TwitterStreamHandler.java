package twitter.sandbox.stream;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class TwitterStreamHandler {
	
	public static void main(String[] args) {
        // gets Twitter instance with default credentials
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("kzaOEKUyFvawKIGUJvvGQvmZW")
		  .setOAuthConsumerSecret("1LT0Gp6hYKnNXP1hi3WCrphnX0lwhi5qqE0zI9awrVKx8mmXIq")
		  .setOAuthAccessToken("2848928985-ZK5EOS5V7aNhlNCQmsqXT2Jm8pyeKN8DaxpHFYH")
		  .setOAuthAccessTokenSecret("Hr5qeY3Iv8t51tYR4hzVv1Se33zhONOB6zqy3gQ1VyehM");
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		MongoHandler mongoHandler = new MongoHandler();
	    DB db = mongoHandler.getMongoDB();
	    final DBCollection coll = db.getCollection("twitterStatus");
	    
		StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {
	        	ObjectMapper mapper = new ObjectMapper();
	        	try {
					;
					Object o = com.mongodb.util.JSON.parse(mapper.writeValueAsString(status));
				    DBObject dbObj = (DBObject) o;
				    DBObject[] arr = new DBObject[1];
				    arr[0] = dbObj;
				    coll.insert(arr);
				    System.out.println("New tweet: "+status.getCreatedAt());
				} catch (JsonGenerationException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	        	// handle complex stuff here
	        }
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	        	// handle complex stuff here
	        }
	        public void onException(Exception ex) {
	            // handle complex stuff here
	        }
			public void onScrubGeo(long arg0, long arg1) {
				// handle complex stuff here
			}
			public void onStallWarning(StallWarning arg0) {
				// handle complex stuff here
			}
	    };
	    
	    twitterStream.addListener(listener);
	    twitterStream.sample();
	    
    }
}
