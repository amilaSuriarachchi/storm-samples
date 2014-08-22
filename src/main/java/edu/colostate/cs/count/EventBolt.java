package edu.colostate.cs.count;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/20/14
 * Time: 9:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventBolt implements IRichBolt {

    private AtomicLong atomicLong = new AtomicLong();
    private long lastTime = System.currentTimeMillis();

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
//        System.out.println("Got event with thread " + Thread.currentThread().getId() + " in bolt ");
//        System.out.println("Got the event time " + input.getDouble(0) + " key1 " + input.getString(1)
//                + " key2 " + input.getDouble(2) + " key3 " + input.getLong(3) + " key4 " + input.getString(4));

        long currentValue = this.atomicLong.incrementAndGet();
        if ((currentValue % 1000000) == 0) {
            System.out.println("Message Rate ==> " + 1000000000 / (System.currentTimeMillis() - this.lastTime) + " From thread - " + Thread.currentThread().getId());
            this.lastTime = System.currentTimeMillis();
        }
//        this.collector.emit(input, new Values(input.getDouble(0), input.getString(1),
//                                                        input.getDouble(2), input.getLong(3), input.getString(4)));
//        this.collector.ack(input);
    }

    @Override
    public void cleanup() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
