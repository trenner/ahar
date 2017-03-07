package de.tuberlin.cit.storageassistant;

import org.apache.hadoop.conf.Configuration;


public enum ConfigProvider {
    get;
    private Configuration config;

    void init(Configuration configuration) {
        if (this.config == null) {
            this.config = configuration;
        }
    }

    public Configuration getConfig() {
        // ConfigProvider.get.getConfig()
        return config;
    }
}