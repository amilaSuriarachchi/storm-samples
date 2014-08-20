package edu.colostate.cs.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/20/14
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("producer", new EventSpout(), 1);
        builder.setBolt("relay", new RelayBolt(), 1).shuffleGrouping("producer");
        builder.setBolt("counter", new EventBolt(), 1).shuffleGrouping("relay");

        Config conf = new Config();
        conf.setNumWorkers(3);
        try {
            StormSubmitter.submitTopology("wordCount", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }

    }
}
