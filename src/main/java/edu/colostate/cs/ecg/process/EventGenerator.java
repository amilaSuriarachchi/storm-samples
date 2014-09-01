package edu.colostate.cs.ecg.process;

import edu.colostate.cs.ecg.analyse.Record;
import edu.colostate.cs.ecg.analyse.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 8/31/14
 * Time: 4:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventGenerator implements Runnable {

    public static final int MAX_SIZE = 1000;
    private int MESSAGE_BUFFER_SIZE = 500;

    private Queue<Record> messages;
    private boolean isFinished;

    public EventGenerator() {
        this.messages = new LinkedList<Record>();
    }

    public synchronized void addRecords(Record[] records) {
        if (this.messages.size() >= MAX_SIZE) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
            addRecords(records);
        } else {
            for (Record record : records) {
                this.messages.add(record);
            }
            this.notify();
        }

    }

    public synchronized Record getRecord() {

        Record record = this.messages.poll();
        while ((record == null) && !this.isFinished) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
            record = this.messages.poll();
        }
        this.notify();
        return record;

    }

    public void sendMessages() {

        try {

            List<String> commands = new ArrayList<String>();
            commands.add("rdsamp");
            commands.add("-r");
            commands.add("3000762/");
            commands.add("-p");
//            commands.add("-f");
//            commands.add("1000");
//            commands.add("-t");
//            commands.add("1100");
            commands.add("-c");
            commands.add("-s");
            commands.add("II");

            RecordReader recordReader = null;
            long totalMessages = 0;
            Record[] messageBuffer = new Record[MESSAGE_BUFFER_SIZE];
            int bufferPointer = 0;

            try {
                recordReader = new RecordReader(commands, "/s/chopin/a/grad/amilas/granulas/Granules/");
                while (recordReader.hasNext()) {
                    messageBuffer[bufferPointer] = recordReader.next();
                    bufferPointer++;
                    if (bufferPointer == MESSAGE_BUFFER_SIZE) {
                        // this means buffer is full.
                        this.addRecords(messageBuffer);
                        bufferPointer = 0;
                    }
                    totalMessages++;
                    if (totalMessages % 500000 == 0) {
                        System.out.println("Number of messages processed " + totalMessages + " time " + System.currentTimeMillis());
                    }
                }
                recordReader.close();
                this.isFinished = true;
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        sendMessages();
    }
}



