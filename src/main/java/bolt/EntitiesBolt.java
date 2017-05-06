package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class EntitiesBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String decode = tuple.getStringByField("message");

        try {
            JSONObject json = new JSONObject(decode);
            String cadenaUrl;

            cadenaUrl = "https://api.dandelion.eu/datatxt/nex/v1/?social=True&min_confidence=0.6&country=-1&include=image%2Cabstract%2Ctypes%2Ccategories%2Clod&text=";
            cadenaUrl += json.getString("text").replaceAll("[^\\p{Alpha}\\p{Digit}]+","+").replace(' ', '+');
            cadenaUrl += "&token=" + Constantes.Dandelion.TOKEN;
            System.out.println("Cadena URL:" + cadenaUrl);

            URL url = new URL(cadenaUrl);

            HttpURLConnection c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("GET");
            c.setRequestProperty("Content-length", "0");
            c.setUseCaches(false);
            c.setAllowUserInteraction(false);
            c.connect();
            int status = c.getResponseCode();

            StringBuilder sb = new StringBuilder();
            switch (status) {
                case 200:
                case 201:
                    BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line+"\n");
                    }
                    br.close();
            }


            JSONObject jsonResponse = new JSONObject(sb.toString());
            JSONArray results = jsonResponse.getJSONArray("annotations");
            String categoria = "Otro";
            JSONArray entidades = new JSONArray();
            for (int i=0; i<results.length();i++) {
                JSONObject annotation = results.getJSONObject(0);
                String nombre = annotation.getString("title");
                JSONArray tipos = annotation.getJSONArray("types");
                for (int j= 0; j< tipos.length(); j++) {
                    String tipo = tipos.getString(j).substring(28);
                    System.out.println("Tipo:" + tipo);
                    if (tipo.equals("Place") || tipo.equals("Location") )
                        categoria = "Place";
                }
                JSONObject entidad = new JSONObject();
                entidad.put("nombre", nombre);
                entidad.put("categoria", categoria);
                entidades.put(entidad);
            }

            json.append("entidades", entidades);

            _collector.emit(new Values(json.toString()));

            // Confirm that this tuple has been treated.
            _collector.ack(tuple);

        }
        catch( JSONException | IOException e){
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
