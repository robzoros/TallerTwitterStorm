package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;


public final class CategoriaBolt extends BaseRichBolt {

    public CategoriaBolt() {
        super();
    }

    private OutputCollector _collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("message");

        //System.out.println("TWEET: " + tweet);

        JSONObject twJson = null;
        try {
            twJson = new JSONObject(tweet);
            String username = null;
            username = twJson.getJSONObject("user").getString("screen_name");

            System.out.println("USER: " + username);
            // URL para obtener key: Obtenemos el kloutid del usuario de twitter (campo id del jason)
            //http://api.klout.com/v2/identity.json/twitter?screenName=(usuario_tw)&key=694cm5meu365uh3k9r8d4x23
            // Connect to the URL using java's native library
            URL url = null;
            url = new URL("http://api.klout.com/v2/identity.json/twitter?screenName=" + username + "&key=" + Constantes.Klout.APIKEY);
            HttpURLConnection request = null;

            request = (HttpURLConnection) url.openConnection();
            request.connect();

            // wrap the urlconnection in a bufferedreader
            BufferedReader bufferedReader = null;
            bufferedReader = new BufferedReader(new InputStreamReader(request.getInputStream()));

            String line;
            StringBuilder content = new StringBuilder();

            // read from the urlconnection via the bufferedreader
            while ((line = bufferedReader.readLine()) != null)
            {
                content.append(line + "\n");
            }
            bufferedReader.close();


            JSONObject obj = null;
            obj = new JSONObject(content.toString());

            String kloudID = null;
            kloudID = obj.getString("id");

            // URL para obtener categorizacion: (campo bucket del json)
            //http://api.klout.com/v2/user.json/(kloud_id)/score?key=694cm5meu365uh3k9r8d4x23
            URL url2 = null;
            url2 = new URL("http://api.klout.com/v2/user.json/" + kloudID + "/score?key=" + Constantes.Klout.APIKEY);
            HttpURLConnection request2 = null;
            request2 = (HttpURLConnection) url2.openConnection();
            request2.connect();

            // wrap the urlconnection in a bufferedreader
            BufferedReader bufferedReader2 = null;
            bufferedReader2 = new BufferedReader(new InputStreamReader(request2.getInputStream()));

            String line2;
            StringBuilder content2 = new StringBuilder();

            // read from the urlconnection via the bufferedreader
            while ((line2 = bufferedReader2.readLine()) != null)
            {
                content2.append(line2 + "\n");
            }
            bufferedReader2.close();

            // Convert to a JSON object to print data
            JSONObject obj2 = null;
            obj2 = new JSONObject(content2.toString());

            String kloudCat = null;
            kloudCat = obj2.getString("bucket");
            System.out.println("BUCKET:     " + kloudCat);

            //TODO Emit both the incoming text and the detected language

            twJson.append ("categoria", kloudCat);

        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }

        _collector.emit(new Values(twJson.toString()));

        //TODO Confirm that this tuple has been treated.
        _collector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json"));
    }
}