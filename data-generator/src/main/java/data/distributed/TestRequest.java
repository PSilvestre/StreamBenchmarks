package data.distributed;

import data.source.model.EventGenerator;

import java.io.Serializable;

public class TestRequest implements Serializable {
    private double totalThroughput;
    private int numParticipatingNodes;
    private double singleNodeThroughput;
    private long testDuration;
    private EventGenerator generatorToUse;


    public TestRequest(double totalThroughput, int numParticipatingNodes, long testDuration,EventGenerator generatorToUse) {
        this.totalThroughput = totalThroughput;
        this.numParticipatingNodes = numParticipatingNodes;
        this.singleNodeThroughput = totalThroughput/numParticipatingNodes;
        this.testDuration = testDuration;
        this.generatorToUse =generatorToUse;
    }

    public double getTotalThroughput() {
        return totalThroughput;
    }

    public void setTotalThroughput(double totalThroughput) {
        this.totalThroughput = totalThroughput;
    }

    public int getNumParticipatingNodes() {
        return numParticipatingNodes;
    }

    public void setNumParticipatingNodes(int numParticipatingNodes) {
        this.numParticipatingNodes = numParticipatingNodes;
    }

    public double getSingleNodeThroughput() {
        return singleNodeThroughput;
    }

    public void setSingleNodeThroughput(double singleNodeThroughput) {
        this.singleNodeThroughput = singleNodeThroughput;
    }

    public long getTestDuration() {
        return testDuration;
    }

    public void setTestDuration(long testDuration) {
        this.testDuration = testDuration;
    }

    public EventGenerator getGeneratorToUse() {
        return generatorToUse;
    }

    public void setGeneratorToUse(EventGenerator generatorToUse) {
        this.generatorToUse = generatorToUse;
    }
}
