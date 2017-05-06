package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.BSON;
import org.bson.Document;

import java.util.Map;

/**
 * Created by utad on 6/05/17.
 */
public class MongoCatBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private MongoCollection<Document> col;
    private MongoDatabase db;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient(Constantes.DB.Connection.URL);

            // Now connect to your databases
            db = mongoClient.getDatabase(Constantes.DB.Connection.NAME);
            System.out.println("Connect to database successfully");

            col = db.getCollection("tweeters");
        }
        catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String decode = tuple.getStringByField("json");

        try {
            Document object = (Document) JSON.parse(decode);
            col.insertOne(object);

            // Confirm that this tuple has been treated.
            _collector.ack(tuple);

        }
        catch( Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
