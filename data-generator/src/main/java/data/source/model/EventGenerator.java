package data.source.model;

import java.io.Serializable;

public interface EventGenerator extends Serializable {

    public Event generateEvent();

}
