package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.util.JSON;
import org.apache.commons.lang.ObjectUtils;
import org.bson.BSON;
import org.bson.Document;
import twitter4j.JSONObject;

import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by utad on 6/05/17.
 */
public class MongoCatBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private MongoCollection<Document> col;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            // To connect to mongodb server
            MongoClientURI connectionString = new MongoClientURI(Constantes.DB.Connection.URL);
            MongoClient mongoClient = new MongoClient(connectionString);
            MongoDatabase db;

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
            Document json = Document.parse(decode);
            Long id = json.getLong("id");
            Document object = new Document();

            if (json.get("categoria") != null) object.append("categoria", json.get("categoria"));
            if (json.get("location") != null) object.append("location", json.get("location"));
            if (json.get("entidades") != null) object.append("entidades", json.get("entidades"));
            object.append("text", json.get("text"));
            object.append("lang", json.get("lang"));
            object.append("created_at", json.get("created_at"));
            col.updateOne(eq("_id", id), new Document("$set", object), (new UpdateOptions()).upsert(true));

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
