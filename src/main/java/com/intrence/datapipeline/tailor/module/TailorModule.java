/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigList;
import com.intrence.config.collection.ConfigMap;
import com.intrence.config.configloader.ConfigMapLoader;
import com.intrence.config.configloader.YamlFileConfigMapLoader;
import com.intrence.core.elasticsearch.ElasticSearchConfiguration;
import com.intrence.datapipeline.tailor.TailorConfiguration;
import com.intrence.datapipeline.tailor.exception.InternalServerException;
import com.intrence.datapipeline.tailor.net.WebFetcher;
import com.intrence.datapipeline.tailor.net.WebFetcherConfig;
import com.intrence.datapipeline.tailor.queue.FetchRequestQueue;
import com.intrence.datapipeline.tailor.queue.RedisFetchRequestQueue;
import com.intrence.datapipeline.tailor.util.Constants;
import io.dropwizard.setup.Environment;
import redis.clients.jedis.JedisPool;

public class TailorModule extends AbstractModule {

    private final TailorConfiguration conf;
    private final Environment env;

    public TailorModule(TailorConfiguration conf, Environment env) {
        this.conf = conf;
        this.env = env;
    }

    @Override
    protected void configure() {
        // Configuration
        bind(WebFetcherConfig.class).toInstance(conf.getWebFetcherConfig());

        // Redis & Queue
        JedisPool jedisPool = conf.getJedisFactory().build(env);
        bind(JedisPool.class).toInstance(jedisPool);
        bind(FetchRequestQueue.class).to(RedisFetchRequestQueue.class).asEagerSingleton();

        // Net
        bind(WebFetcher.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @Named("SourcesConfigMap")
    ConfigMap provideSourcesConfigMap() {
        ConfigMapLoader configMapLoader = null;
        if (Constants.DEV_ENVIRONMENTS.contains(ConfigProvider.getEnvironment())) {
            configMapLoader = new YamlFileConfigMapLoader(Constants.CDG_SOURCES_CONFIG_LOCALHOST_PATH);
        } else {
//            configMapLoader = new LocalFileConfigMapLoader(Constants.DORA_SOURCES_CONFIG_LOCALHOST_PATH, 300, new SourcesConfigUpdateHandler());
        }

        ConfigMap configMap = configMapLoader.getConfigMap();
        ConfigMap supportedSources = configMap.getMap(Constants.SUPPORTED_SOURCES);

        if( supportedSources == null || supportedSources.isEmpty() ) {
            throw new ExceptionInInitializerError("Event=Error initializing configMap..  Sources config is empty!!\n" +
                    "Bad Config provided.");
        }

        for (String source: supportedSources.keySet()) {
            String sourceIntegrationType = supportedSources.getMap(source).getString(Constants.SOURCE_INTEGRATION_TYPE, null);
            ConfigList sourceOperationTypes = supportedSources.getMap(source).getList(Constants.SOURCE_OPERATION_TYPES, new ConfigList());
            if(sourceIntegrationType==null) {
                throw new InternalServerException(String.format("Integration type for source=%s is not provided in Dora config file.", source));
            }
            if(sourceOperationTypes.size()<1) {
                throw new InternalServerException(String.format("Operation type for source=%s is not provided in Dora config file.", source));
            }
        }

        return supportedSources;
    }

}
