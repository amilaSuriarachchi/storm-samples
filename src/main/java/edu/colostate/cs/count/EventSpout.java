package edu.colostate.cs.count;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/20/14
 * Time: 9:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Values event;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "key1", "key2", "key3", "key4"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.event = new Values(1000.00, "value1", 4567.89, 100000l, "Last value to send");
    }

    @Override
    public void nextTuple() {
        for (int i = 0; i < 100; i++) {
            this.collector.emit(this.event);
        }

    }
}
