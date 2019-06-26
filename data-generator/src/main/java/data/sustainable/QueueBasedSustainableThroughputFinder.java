package data.sustainable;

import data.source.model.EventGenerator;
import data.source.socket.BufferReaderThread;
import data.source.socket.DataGeneratorThread;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

/**
 * Finds maximum sustainable rate following the methods shown in http://asterios.katsifodimos.com/assets/publications/icde18-benchmarks.pdf
 * <p>
 * The queue of events is split into three areas.
 * |-----------------------------------------------|
 * |    GREEN    |       YELLOW       |    RED     |
 * |_______________________________________________|
 * <p>
 * If it is always in the green area it is sustainable.
 * If it enters yellow, it may stay there for a duration and a maximum number of pushes to the queue.
 * If it enters red, it is unsustainable.
 */
public class QueueBasedSustainableThroughputFinder implements SustainableThroughputFinder {

    public int START_THROUGHPUT = 5000;
    public int START_THROUGHPUT_DECREMENT = START_THROUGHPUT / 10;

    private int maximumPushesWhileInYellowArea;
    private long maxTimeInYellow;
    private float greenSizeRatio;
    private float yellowSizeRatio;

    private int currentThroughput;
    private int currentThroughputDecrement;


    public QueueBasedSustainableThroughputFinder(int maximumPushesWhileInYellowArea, long maxTimeInYellow, float greenSizeRatio, float yellowSizeRatio) {
        this.maximumPushesWhileInYellowArea = maximumPushesWhileInYellowArea;
        this.maxTimeInYellow = maxTimeInYellow;
        this.greenSizeRatio = greenSizeRatio;
        this.yellowSizeRatio = yellowSizeRatio;

        this.currentThroughput = START_THROUGHPUT;
        this.currentThroughputDecrement = START_THROUGHPUT_DECREMENT;
    }

    @Override
    public float findSustainableThroughput(Socket connection, EventGenerator eventGenerator, HashMap conf, BlockingQueue<String> buffer) throws IOException {
        PrintWriter out = new PrintWriter(connection.getOutputStream(), true);
        int bufferSize = (int) conf.get("benchmarking.count");
        int yellowStartIndex = (int) (bufferSize * greenSizeRatio);
        int redStartIndex = yellowStartIndex + ((int) (bufferSize * yellowSizeRatio));
        long sleepBetweenPushes = new Long(conf.get("datagenerator.sleep").toString());
        long suspectWaitTimeMS = ((redStartIndex - yellowStartIndex) - yellowStartIndex)/2 * sleepBetweenPushes;

        boolean finished = false;

        while (!finished) {
            try {
                Thread generator = new DataGeneratorThread(eventGenerator, conf, buffer);
                generator.start();
                Thread bufferReader = new BufferReaderThread(buffer, conf, out);
                bufferReader.start();

                boolean isSustainable = true;
                boolean isSuspect = false;
                long suspectStartTime = 0;
                while (generator.isAlive()) {
                    if(buffer.size() >= redStartIndex){
                        isSustainable = false;
                        break;
                    }

                    if(!isSuspect && buffer.size() > yellowStartIndex) {
                        isSuspect = true;
                        suspectStartTime = System.currentTimeMillis();
                    }

                    if(isSuspect &&  System.currentTimeMillis() > suspectStartTime + suspectWaitTimeMS){
                        if(buffer.size() > yellowStartIndex){
                            isSustainable = false;
                            break;
                        }
                        else {
                            isSuspect = false;
                        }
                    }
                    Thread.sleep(sleepBetweenPushes/2); //sleep half as long as a push
                }
                generator.join();
                bufferReader.join();
                if(isSustainable) {
                    break;
                }
                currentThroughput -= currentThroughputDecrement; //TODO do a finer pass?

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return currentThroughput; //TODO reset currentThroughput
    }
}
