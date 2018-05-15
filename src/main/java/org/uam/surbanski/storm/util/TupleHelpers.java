package org.uam.surbanski.storm.util;

import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;

public final class TupleHelpers {

    public static boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}
