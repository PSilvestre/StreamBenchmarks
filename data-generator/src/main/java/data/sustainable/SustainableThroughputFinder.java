package data.sustainable;

import data.source.model.EventGenerator;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

/**
 * Sustainable throughput is defined to be  the highest load of event traffic that a system
 * can handle without exhibiting prolonged backpressure (i.e. without a continuously increasing event-time latency)
 */
public interface SustainableThroughputFinder {

    float findSustainableThroughput(Socket connection, EventGenerator eventGenerator, HashMap conf) throws IOException;

}
