package edu.colostate.cs.ecg.process;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import edu.colostate.cs.ecg.analyse.Record;
import edu.colostate.cs.ecg.analyse.RecordReader;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 7/31/14
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventProducer extends BaseRichSpout {

    private RecordReader recordReader;
    private SpoutOutputCollector spoutOutputCollector;

    private long startTime;
    private long numberOfMsg;

    public void createRecordReader() {

        List<String> commands = new ArrayList<String>();
        commands.add("rdsamp");
        commands.add("-r");
        commands.add("3000762/");
        commands.add("-p");
//        commands.add("-f");
//        commands.add("1000");
//        commands.add("-t");
//        commands.add("1100");
        commands.add("-c");
        commands.add("-s");
        commands.add("II");

        try {
            this.recordReader = new RecordReader(commands, "/s/chopin/a/grad/amilas/granulas/Granules/");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.TIME, Constants.VALUE, Constants.STREAM_ID));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        createRecordReader();
        this.numberOfMsg = 0;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void nextTuple() {
        Record record = null;

        if (this.recordReader.hasNext()) {
            record = this.recordReader.next();
            for (int i = 0; i < 20; i++) {
                this.spoutOutputCollector.emit(new Values(record.getTime(), record.getValue(), "ecg" + i));
                this.numberOfMsg++;
            }
        } else {
            // we have come to end calculate the performance
            double throughput = this.numberOfMsg * 1000.0 / (System.currentTimeMillis() - this.startTime);
            try {
                System.out.println("Throughput ==> " + throughput + " " + InetAddress.getLocalHost().getHostName());
            } catch (UnknownHostException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            try {
                FileWriter fileWriter = new FileWriter("/tmp/amilas/result.txt");
                fileWriter.write("Throughput ==> " + throughput);
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            this.recordReader.close();
            Utils.sleep(100000000);

        }
    }
}
