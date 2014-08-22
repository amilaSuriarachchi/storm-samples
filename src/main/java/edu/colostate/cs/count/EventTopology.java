package edu.colostate.cs.count;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

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
        builder.setSpout("producer", new EventSpout(), 2);
//        builder.setBolt("relay", new RelayBolt(), 1).shuffleGrouping("producer");
        builder.setBolt("counter", new EventBolt(), 2).shuffleGrouping("producer");

        Config conf = new Config();
        conf.setNumWorkers(4);
        try {
            StormSubmitter.submitTopology("wordCount", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }

    }
}
