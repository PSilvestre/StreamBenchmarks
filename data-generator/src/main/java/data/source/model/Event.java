package data.source.model;

public class Event {
    private String key;
    private long ts;
    private String value;

    public Event(String key, String value) {
        this.key = key;
        this.value = value;
        this.ts = System.currentTimeMillis();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "{ \"key\":\"" + key + "\",\"value\":\"" + value + "\",\"ts\": \"" + this.ts + "\"}";
    }
}
