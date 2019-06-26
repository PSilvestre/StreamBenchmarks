package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEventGenerator;
import data.source.model.EventGenerator;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGeneratorThread extends Thread {
    private int benchmarkCount;
    private long sleepTime;
    private static Double partition;
    private BlockingQueue<String> buffer;
    private EventGenerator eventGenerator;
    private HashMap<Long, Integer> bufferSizeAtTime = new HashMap<>();

    private HashMap<Long, Integer> dataGenRate = new HashMap<>();

    public DataGeneratorThread(EventGenerator eventGenerator, HashMap conf, BlockingQueue<String> buffer) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.eventGenerator = eventGenerator;
    }

    public void run() {
        try {
            sendTuples(benchmarkCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount) throws Exception {
        long currTime = System.currentTimeMillis();
        int tempVal = 0;
        if (sleepTime != 0) {
            for (int i = 0; i < tupleCount; ) {
                Thread.sleep(sleepTime);
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) { //TODO what is going on here
                    buffer.put(this.eventGenerator.generateEvent().toString());
                    if (i % 1000 == 0) {
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ) {
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) {
                    buffer.put(this.eventGenerator.generateEvent().toString());
                    if (i % 1000 == 0) {
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        }
        long runtime = (currTime - System.currentTimeMillis()) / 1000;
        System.out.println("Benchmark producer data rate is " + tupleCount / runtime + " ps");
    }
}


