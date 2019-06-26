package data.source.socket;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BufferReaderThread extends Thread {
    private BlockingQueue<String> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private PrintWriter out;
    private int benchmarkCount;
    private HashMap<Long, Integer> thoughputCount = new HashMap<>();

    public BufferReaderThread(BlockingQueue<String> buffer, HashMap conf, PrintWriter out) {
        this.buffer = buffer;
        this.out = out;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
    }

    public void run() {
        try {
            long timeStart = System.currentTimeMillis();

            int tempVal = 0;
            for (int i = 0; i < benchmarkCount; i++) {
                String tuple = buffer.take();
                out.println(tuple);
                if (i % 1000 == 0) {
                    thoughputCount.put(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), i - tempVal);
                    tempVal = i;
                    logger.info(i + " tuples sent from buffer");
                }
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = benchmarkCount / runtime;

            logger.info("---BENCHMARK ENDED--- on " + runtime + " seconds with " + throughput + " throughput "
                    + " node : " + InetAddress.getLocalHost().getHostName());


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void writeHashMapToCsv(HashMap<Long, Integer> hm, String path) {
        try {
            File file = new File(path.split("\\.")[0] + "-" + InetAddress.getLocalHost().getHostName() + ".csv");

            if (file.exists()) {
                file.delete(); //you might want to check if delete was successfull
            }
            file.createNewFile();
            FileOutputStream fileOutput = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutput));
            Iterator it = hm.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry) it.next();
                bw.write(pair.getKey() + "," + pair.getValue() + "\n");
            }
            bw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
