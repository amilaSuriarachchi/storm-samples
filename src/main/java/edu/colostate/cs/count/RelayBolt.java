package edu.colostate.cs.count;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/20/14
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelayBolt implements IRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

//        System.out.println("Got event with thread " + Thread.currentThread().getId() + " in relay ");
//        System.out.println("Got the event time " + tuple.getDouble(0) + " key1 " + tuple.getString(1)
//                + " key2 " + tuple.getDouble(2) + " key3 " + tuple.getLong(3) + " key4 " + tuple.getString(4));
        this.outputCollector.emit(tuple, new Values(tuple.getDouble(0), tuple.getString(1),
                                           tuple.getDouble(2), tuple.getLong(3), tuple.getString(4)));
//        this.outputCollector.ack(tuple);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void cleanup() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time","key1","key2","key3","key4"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
