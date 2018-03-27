package com.intrence.datapipeline.tailor.streamer;


import com.intrence.datapipeline.tailor.exception.TailorBackendException;
import com.intrence.datapipeline.tailor.streamer.provider.StreamProvider;

/**
 * Created by ajijhotiya on 16/12/16.
 */
public enum StreamType {

    CSV("csv", CSVStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws TailorBackendException {
            return new CSVStreamer(streamProvider);
        }
    },
    XML("xml", XMLStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws TailorBackendException {
            return new XMLStreamer(streamProvider);
        }
    },
    JSON("json", JSONStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws TailorBackendException {
            return new JSONStreamer(streamProvider);
        }
    };

    private String streamType;
    private Class<? extends Streamer> streamImplClass;

    StreamType(String streamType, Class<? extends Streamer> streamImplClass) {
        this.streamType = streamType;
        this.streamImplClass = streamImplClass;
    }

    public abstract Streamer getStreamerInstance(StreamProvider streamProvider) throws TailorBackendException;

    public static StreamType fromString(String streamType) {
        if (streamType != null) {
            for (StreamType streamValue : StreamType.values()) {
                if (streamType.equalsIgnoreCase(streamValue.streamType)) {
                    return streamValue;
                }
            }
        }
        throw new IllegalArgumentException(String.format("Invalid streamer type string %s", streamType));
    }

}
