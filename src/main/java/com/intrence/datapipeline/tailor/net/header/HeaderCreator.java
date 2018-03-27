package com.intrence.datapipeline.tailor.net.header;

import org.apache.http.client.methods.RequestBuilder;

public interface HeaderCreator {
    void addDynamicHeader(String source, RequestBuilder requestBuilder) throws Exception;
}
