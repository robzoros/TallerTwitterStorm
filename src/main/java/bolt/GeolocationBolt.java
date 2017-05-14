package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import twitter4j.JSONArray;
import twitter4j.JSONObject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by utad on 5/05/17.
 */
public class GeolocationBolt extends BaseRichBolt {
    private OutputCollector _collector;

    private String lat = "0.0";
    private String lng = "0.0";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        //String text = tuple.getStringByField("message");
        //StringTokenizer st = new StringTokenizer(text);

        //String jsonS = tuple.getString(0);
        String url = "";
        try {
            JSONObject json = new JSONObject(tuple.getString(0));

            /*
            {
                entidades: [ {
                            nombre: Madrid
                            categoria: Place
                           }
                           {
                            nombre2: Gato
                            categoria2: Animal
                           }
                      ]
            }
             */

            //System.out.println(json.toString());
            JSONArray jarray = json.getJSONArray("entidades");
            //System.out.println(jarray.toString());
            JSONArray salida = new JSONArray();

            for (int i = 0; i< jarray.length(); i++)
            {

                JSONObject jobject = jarray.getJSONObject(i);
                String cat = jobject.getString("categoria");
                String nombre = jobject.getString("nombre");
                nombre = nombre.replace(' ', '+');
                System.out.println(nombre);
                if (cat.equals("Place"))
                {
                    url = "http://maps.google.com/maps/api/geocode/json?address="+nombre+"&sensor=false";
                    URL peticion = new URL(url);
                    HttpURLConnection connection = (HttpURLConnection) peticion.openConnection();
                    connection.connect();

                    BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String inputLine;
                    String response = "";
                    while ((inputLine = br.readLine()) != null)
                        response += inputLine;

                    JSONObject jsongoogle = new JSONObject(response);
                    JSONArray jarraygoogle = jsongoogle.getJSONArray("results");
                    JSONObject jobjectgoogle = jarraygoogle.getJSONObject(0);
                    JSONObject geo = jobjectgoogle.getJSONObject("geometry").getJSONObject("location");
                    this.lat = geo.getString("lat");
                    this.lng = geo.getString("lng");

                    JSONObject sal = new JSONObject();

                    sal.append("lat", this.lat);
                    sal.append("lng", this.lng);
                    sal.append("nombre", nombre);

                    salida.put(sal);
                }
            }


            json.put("location", salida);

            /*
            {
                location: [{
                                nombre:
                                lat:
                                lng
                             }
                             {
                                nombre:
                                lat:
                                lng:
                             }
                          ]
             }
            */

            //System.out.println();
            _collector.emit(new Values(json.toString()));
            // Confirm that this tuple has been treated.
            _collector.ack(tuple);

        } catch (twitter4j.JSONException | IOException e){
            System.out.println("URL: " + url);
            e.printStackTrace();
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