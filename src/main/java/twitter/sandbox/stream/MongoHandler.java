package twitter.sandbox.stream;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoHandler {
	
	public DB getMongoDB(){
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient( "localhost" );
			DB db = mongoClient.getDB( "twitterDb" );
			return db;
		} catch (UnknownHostException e) {
			return null;
		}
	}
}
