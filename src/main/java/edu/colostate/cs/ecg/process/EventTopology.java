package edu.colostate.cs.ecg.process;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/21/14
 * Time: 2:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("producer", new EventProducer(), 1);
        builder.setBolt("receiver", new EventReceiver(), 4).fieldsGrouping("producer", new Fields(Constants.STREAM_ID));

        Config conf = new Config();
        conf.setNumWorkers(2);
        try {
            StormSubmitter.submitTopology("ecg", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
