/**
 * Created by wliu on 12/13/17.
 */
package com.intrence.datapipeline.tailor.net;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FetchRequestTest {

    private static Map<String, String> inputParameters = new HashMap<String, String>() {{
        put("test_key1", "test_value1");
        put("test_key2", "test_value2");
    }};

    public static FetchRequest fetchRequestExample = new FetchRequest.Builder()
            .workRequest("test_workRequest")
            .methodType("GET")
            .priority(0)
            .inputParameters(inputParameters)
            .httpBody("test_httpBody")
            .httpResponse("test_httpResponse")
            .build();

    public static String fetchRequestExampleJson = "{\"workRequest\":\"test_workRequest\",\"priority\":0,\"methodType\":\"GET\",\"httpBody\":\"test_httpBody\",\"httpResponse\":\"test_httpResponse\",\"inputParameters\":{\"test_key2\":\"test_value2\",\"test_key1\":\"test_value1\"}}";

    @Test
    public void testSerializeFetchRequest() throws Exception {
        String json = fetchRequestExample.toJson();
        Assert.assertEquals(fetchRequestExampleJson, json);
    }

    @Test
    public void testDeserializeFetchRequest() throws Exception {
        FetchRequest fetchRequest = FetchRequest.fromJson(fetchRequestExampleJson);
        Assert.assertEquals("test_workRequest", fetchRequest.getWorkRequest());
        Assert.assertEquals(inputParameters, fetchRequest.getInputParameters());
    }
}
