package com.intrence.datapipeline.tailor.streamer;


import com.intrence.datapipeline.tailor.exception.TailorBackendException;
import com.intrence.datapipeline.tailor.streamer.provider.StreamProvider;
import org.apache.commons.lang3.NotImplementedException;

/**
 *      Created by ajijhotiya on 22/11/16.
 *
 *      This is concrete implementation of JSON Streamer. This class contains the logic of reading JSON data as stream
 *      from file.
 */
public class JSONStreamer extends Streamer {

    protected JSONStreamer(StreamProvider streamProvider) throws TailorBackendException {
        super(streamProvider);
    }

    @Override
    public boolean hasNext() throws TailorBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public String next() throws TailorBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public void close() throws TailorBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }
}
