package data.distributed;

import java.io.Serializable;
import java.util.Map;

public class TestResult implements Serializable {
    private boolean isSustainable;
    private Map<Long, Integer> throughputCount;
    public TestResult(boolean isSustainable,  Map<Long, Integer> throughputCount) {
        this.isSustainable = isSustainable;
        this.throughputCount = throughputCount;
    }

    public boolean isSustainable() {
        return isSustainable;
    }

    public void setSustainable(boolean sustainable) {
        isSustainable = sustainable;
    }

    public Map<Long, Integer> getThroughputCount() {
        return throughputCount;
    }

    public void setThroughputCount(Map<Long, Integer> throughputCount) {
        this.throughputCount = throughputCount;
    }
}
