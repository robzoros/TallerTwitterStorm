import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.*;
import spout.TwitterSpout;
import twitter4j.FilterQuery;


public class TwitterTopologia {

    private static String consumerKey ;
    private static String consumerSecret;
    private static String accessToken;
    private static String accessTokenSecret;



    public static void main(String[] args) throws Exception {
        /**************** SETUP ****************/
        String remoteClusterTopologyName = null;
        consumerKey = Constantes.TweeterCredentials.consumerKey;
        consumerSecret = Constantes.TweeterCredentials.consumerSecret;
        accessToken = Constantes.TweeterCredentials.accessToken;
        accessTokenSecret = Constantes.TweeterCredentials.accessTokenSecret;

        if (args!=null) {
            if (args.length==1) {
                remoteClusterTopologyName = args[0];
            }
            // If credentials are provided as commandline arguments
            else if (args.length==4) {
                consumerKey =args[0];
                consumerSecret =args[1];
                accessToken =args[2];
                accessTokenSecret =args[3];
            }

        }
        /****************       ****************/

        TopologyBuilder builder = new TopologyBuilder();

        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.track(new String[]{"Madrid"});
        // See https://github.com/kantega/storm-twitter-workshop/wiki/Basic-Twitter-stream-reading-using-Twitter4j


        TwitterSpout spout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, tweetFilterQuery);
        CategoriaBolt categoria = new CategoriaBolt();
        EntitiesBolt entidades = new EntitiesBolt();
        GeolocationBolt geolocation = new GeolocationBolt();
        MongoCatBolt mongo = new MongoCatBolt();
        FileWriterBolt fileWriterBolt = new FileWriterBolt("/tmp/twitter.txt");

        builder.setSpout("spoutLeerTwitter",spout,1);
        builder.setBolt("entidades",entidades,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("categoria",categoria,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("geolocation",geolocation,1).shuffleGrouping("entidades");
        builder.setBolt("mongo",mongo,1).shuffleGrouping("geolocation");
        builder.setBolt("mongo2",mongo,1).shuffleGrouping("categoria");
        //builder.setBolt("escribir2",fileWriterBolt,1).shuffleGrouping("spoutLeerTwitter");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter-fun", conf, builder.createTopology());

            Thread.sleep(460000);

            cluster.shutdown();
        }
    }
}
