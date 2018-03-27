/**
 * Created by wliu on 12/12/17.
 */
package com.intrence.datapipeline.tailor.net;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class WebFetcherConfig {

    @NotNull
    @JsonProperty
    Boolean useProxyService;

    @NotNull
    @JsonProperty
    String pxProxyHost;

    @NotNull
    @JsonProperty
    Integer pxProxyPort;

    @JsonProperty
    String pxProxyUser;

    @JsonProperty
    String pxProxyPassword;



    public Boolean getUseProxyService() {
        return this.useProxyService;
    }

    public String getPxProxyHost() {
        return this.pxProxyHost;
    }

    public Integer getPxProxyPort() {
        return this.pxProxyPort;
    }

    public String getPxProxyUser() {
        return this.pxProxyUser;
    }

    public String getPxProxyPassword() {
        return this.pxProxyPassword;
    }
}
