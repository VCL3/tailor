package com.intrence.datapipeline.tailor;

import com.bendb.dropwizard.redis.JedisFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.intrence.core.elasticsearch.ElasticSearchConfiguration;
import com.intrence.core.persistence.postgres.PostgresConfig;
import com.intrence.datapipeline.tailor.net.WebFetcherConfig;
import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.*;

import javax.validation.Valid;
import javax.validation.constraints.*;

public class TailorConfiguration extends Configuration {

    private final PostgresConfig postgresConfig;
    private final ElasticSearchConfiguration elasticSearchConfig;
    private final JedisFactory jedisFactory;
    private final WebFetcherConfig webFetcherConfig;

    @JsonCreator
    public TailorConfiguration(@JsonProperty(value = "postgres", required = true) PostgresConfig postgresConfig,
                               @JsonProperty(value = "elasticSearch", required = true) ElasticSearchConfiguration elasticSearchConfig,
                               @JsonProperty(value = "redis", required = true) JedisFactory jedisFactory,
                               @JsonProperty(value = "webFetcher", required = true) WebFetcherConfig webFetcherConfig) {
        this.postgresConfig = postgresConfig;
        this.elasticSearchConfig = elasticSearchConfig;
        this.jedisFactory = jedisFactory;
        this.webFetcherConfig = webFetcherConfig;
    }

    public PostgresConfig getPostgresConfig() {
        return this.postgresConfig;
    }

    public ElasticSearchConfiguration getElasticSearchConfig() {
        return this.elasticSearchConfig;
    }

    public JedisFactory getJedisFactory() {
        return this.jedisFactory;
    }

    public WebFetcherConfig getWebFetcherConfig() {
        return this.webFetcherConfig;
    }

}
