/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.datapipeline.tailor.net;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.intrence.datapipeline.tailor.crawler.NormalizeURL;
import com.intrence.models.util.JsonHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;

import java.io.IOException;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = FetchRequest.Builder.class)
public class FetchRequest implements Comparable {

    private String workRequest; //example: urls to crawl, factual_id to fetch etc.
    private Integer priority;
    private String methodType;
    private String httpBody;
    private String httpResponse;
    private Map<String,String> inputParameters;

    public FetchRequest() {}

    public FetchRequest(Builder builder) {
        this.workRequest = builder.workRequest;
        this.priority = builder.priority;
        this.methodType = builder.methodType;
        this.httpBody = builder.httpBody;
        this.httpResponse = builder.httpResponse;
        this.inputParameters = builder.inputParameters;
    }

    public FetchRequest(Map<String,String> inputParameters) {
        this.inputParameters = inputParameters;
    }

    public FetchRequest(String workRequest, int priority) {
        this(workRequest, priority, HttpGet.METHOD_NAME, null);
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody) {
        if (StringUtils.isEmpty(workRequest)) {
            throw new IllegalArgumentException("Cannot construct fetchReq with empty/null workRequest");
        }

        try {
            if(workRequest.startsWith("http")) {
                this.workRequest = NormalizeURL.normalize(workRequest);
            } else {
                this.workRequest = workRequest;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.priority = priority;
        this.methodType = methodType;

        if (StringUtils.isNotBlank(httpBody)) {
            this.httpBody = httpBody;
        }
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody,Map<String,String> inputParameters) {
        this(workRequest, priority, methodType, httpBody);
        this.inputParameters = inputParameters;
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody,Map<String,String> inputParameters, String httpResponse) {
        this(workRequest, priority, methodType, httpBody, inputParameters);
        this.httpResponse = httpResponse;
    }

    @JsonProperty
    public String getWorkRequest() {
        return this.workRequest;
    }

    @JsonProperty
    public Integer getPriority() {
        return this.priority;
    }

    @JsonProperty
    public String getMethodType() {
        return this.methodType;
    }

    @JsonProperty
    public String getHttpBody() {
        return this.httpBody;
    }

    @JsonProperty
    public String getHttpResponse() {
        return this.httpResponse;
    }

    @JsonProperty
    public Map<String, String> getInputParameters() {
        return this.inputParameters;
    }

    @Override
    public int compareTo(Object o) {
        if (this == o) return 0;
        if (o == null || getClass() != o.getClass()) throw new IllegalArgumentException("Object not comparable with FetchRequest");

        FetchRequest that = (FetchRequest) o;
        if (this.priority < that.priority) {
            return -1;
        } else if (this.priority > that.priority) {
            return 1;
        } else {
            return this.workRequest.compareTo(that.workRequest);
        }
    }

    public String toJson() throws JsonProcessingException {
        return JsonHandler.getInstance().convertObjectToJsonString(this);
    }

    public static FetchRequest fromJson(String jsonString) {
        try {
            return JsonHandler.getInstance().convertJsonStringToObject(jsonString, FetchRequest.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String toString() {
        return "FetchRequest{" +
                "workRequest='" + workRequest + '\'' +
                ", priority=" + priority +
                ", methodType='" + methodType + '\'' +
                ", httpBody='" + httpBody + '\'' +
                ", httpResponse='" + httpResponse + '\'' +
                ", inputParameters=" + inputParameters +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchRequest that = (FetchRequest) o;

        if (workRequest != null ? !workRequest.equals(that.workRequest) : that.workRequest != null) return false;
        if (methodType != null ? !methodType.equals(that.methodType) : that.methodType != null) return false;
        if (httpBody != null ? !httpBody.equals(that.httpBody) : that.httpBody != null) return false;
        if (httpResponse != null ? !httpResponse.equals(that.httpResponse) : that.httpResponse != null) return false;
        if (inputParameters != null ? !inputParameters.equals(that.inputParameters) : that.inputParameters != null) return false;
        if (priority.equals(that.priority)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = workRequest != null ? workRequest.hashCode() : 0;
        result = 31 * result + (methodType != null ? methodType.hashCode() : 0);
        result = 31 * result + (httpBody != null ? httpBody.hashCode() : 0);
        result = 31 * result + (httpResponse != null ? httpResponse.hashCode() : 0);
        result = 31 * result + (inputParameters != null ? inputParameters.hashCode() : 0);
        result = 31 * result + priority;
        return result;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {

        private String workRequest; //example: urls to crawl, factual_id to fetch etc.
        private Integer priority;
        private String methodType;
        private String httpBody;
        private String httpResponse;
        private Map<String,String> inputParameters;

        public Builder() {
        }

        public Builder(FetchRequest fetchRequest) {
            this.workRequest = fetchRequest.workRequest;
            this.priority = fetchRequest.priority;
            this.methodType = fetchRequest.methodType;
            this.httpBody = fetchRequest.httpBody;
            this.httpResponse = fetchRequest.httpResponse;
            this.inputParameters = fetchRequest.inputParameters;
        }

        @JsonSetter
        public Builder workRequest(String workRequest) {
            this.workRequest = workRequest;
            return this;
        }

        @JsonSetter
        public Builder priority(Integer priority) {
            this.priority = priority;
            return this;
        }

        @JsonSetter
        public Builder methodType(String methodType) {
            this.methodType = methodType;
            return this;
        }

        @JsonSetter
        public Builder httpBody(String httpBody) {
            this.httpBody = httpBody;
            return this;
        }

        @JsonSetter
        public Builder httpResponse(String httpResponse) {
            this.httpResponse = httpResponse;
            return this;
        }

        @JsonSetter
        public Builder inputParameters(Map<String, String> inputParameters) {
            this.inputParameters = inputParameters;
            return this;
        }

        public FetchRequest build() {
            return new FetchRequest(this);
        }

    }

}

