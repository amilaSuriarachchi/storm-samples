package edu.colostate.cs.ecg.process;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import edu.colostate.cs.ecg.analyse.Record;
import edu.colostate.cs.ecg.analyse.Tompikens;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 7/31/14
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventReceiver implements IRichBolt {

    private Map<String, Tompikens> keyMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.keyMap = new HashMap<String, Tompikens>();
    }

    @Override
    public void execute(Tuple tuple) {

        String key = tuple.getString(2);
        if (!this.keyMap.containsKey(key)) {
            synchronized (this.keyMap) {
                if (!this.keyMap.containsKey(key)) {
                    this.keyMap.put(key, new Tompikens());
                }
            }
        }

        Tompikens tompikens = this.keyMap.get(key);
        Record record = new Record(tuple.getDouble(0),tuple.getDouble(1));
        tompikens.bandPass(record);

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
