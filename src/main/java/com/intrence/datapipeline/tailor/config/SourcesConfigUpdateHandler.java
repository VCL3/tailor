package com.intrence.datapipeline.tailor.config;


import com.intrence.datapipeline.tailor.util.Constants;
import com.intrence.config.collection.ConfigMap;
import com.intrence.config.configloader.ConfigMapUpdateHandler;

import java.util.HashSet;
import java.util.Set;

public class SourcesConfigUpdateHandler implements ConfigMapUpdateHandler {
    private static Set<ConfigMapUpdateHandler> otherListeners = new HashSet<>();
    
    @Override
    public void handleUpdate(ConfigMap configMap) {
        ConfigMap supportedSources = configMap.getMap(Constants.SUPPORTED_SOURCES);
        for (ConfigMapUpdateHandler listener : otherListeners) {
            listener.handleUpdate(supportedSources);
        }
    }
    
    public static void registerConfigUpdateListener(ConfigMapUpdateHandler listener) {
        otherListeners.add(listener);
    }
    
}