package com.github.cleverage.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import play.Logger;
import play.Application;

import java.net.InetAddress;
import java.nio.file.Paths;

public class IndexClient {

    public static org.elasticsearch.node.Node node = null;

    //public static org.elasticsearch.client.Client client = null;
    public static org.elasticsearch.client.RestHighLevelClient client = null;

    public static IndexConfig config;

    public IndexClient(Application application) {
        // ElasticSearch config load from application.conf
        this.config = new IndexConfig(application);
    }

    public void start() throws Exception {

        // Load Elasticsearch Settings
        Settings.Builder settings = loadSettings();

        // Check Model
        if (this.isLocalMode()) {
            Logger.info("ElasticSearch : Starting in Local Mode");
            RestHighLevelClient c = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")));
            //NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
            //node = nb.node();
            /*org.elasticsearch.env.Environment env = new org.elasticsearch.env.Environment(settings.build(), null);
            node = new org.elasticsearch.node.Node(env);
            client = node;*/
            client = c;
            Logger.info("ElasticSearch : Started in Local Mode");
        } else {
            Logger.info("ElasticSearch : Starting in Client Mode");
            //TransportClient c = new PreBuiltTransportClient(settings);
            if (config.client == null) {
                throw new Exception("Configuration required - elasticsearch.client when local model is disabled!");
            }

            String[] hosts = config.client.trim().split(",");
            boolean done = false;

            if(null != hosts && hosts.length > 0) {
                HttpHost[] addresses = new HttpHost[hosts.length];
                int i = 0;
                for (String host : hosts) {
                    String[] parts = host.split(":");
                    if (parts.length != 2) {
                        throw new Exception("Invalid Host: " + host);
                    }
                    Logger.info("ElasticSearch : Client - Host: " + parts[0] + " Port: " + parts[1]);
                    InetAddress hostName = InetAddress.getByName(parts[0]);
                    Integer port = Integer.valueOf(parts[1]);
                    addresses[i] = new HttpHost(hostName, port, "http");
                    //c.addTransportAddress(new TransportAddress(InetAddress.getByName(parts[0]), Integer.valueOf(parts[1])));
                    done = true;
                    i++;
                }

                RestHighLevelClient c = new RestHighLevelClient(RestClient.builder(addresses));
                client = c;
            }
            if (!done) {
                throw new Exception("No Hosts Provided for ElasticSearch!");
            }

            Logger.info("ElasticSearch : Started in Client Mode");
        }

        // Check Client
        /*if (client == null) {
            throw new Exception("ElasticSearch Client cannot be null - please check the configuration provided and the health of your ElasticSearch instances.");
        }*/
    }

    /**
     * Checks if is local mode.
     *
     * @return true, if is local mode
     */
    private boolean isLocalMode() {
        try {
            if (config.client == null) {
                return true;
            }
            if (config.client.equalsIgnoreCase("false") || config.client.equalsIgnoreCase("true")) {
                return true;
            }

            return config.local;
        } catch (Exception e) {
            Logger.error("Error! Starting in Local Model: %s", e);
            return true;
        }
    }

    /**
     * Load settings from resource file
     *
     * @return
     * @throws Exception
     */
    private Settings.Builder loadSettings() throws Exception {
        Settings.Builder settings = Settings.builder();

        // set default settings
        settings.put("client.transport.sniff", config.sniffing);

        if (config.clusterName != null && !config.clusterName.isEmpty()) {
            settings.put("cluster.name", config.clusterName);
        }

        // load settings
        if (config.localConfig != null && !config.localConfig.isEmpty()) {
            Logger.debug("Elasticsearch : Load settings from " + config.localConfig);
            try {
                settings.loadFromPath(Paths.get(this.getClass().getClassLoader().getResource(config.localConfig).toURI()));
            } catch (SettingsException settingsException) {
                Logger.error("Elasticsearch : Error when loading settings from " + config.localConfig);
                throw new Exception(settingsException);
            }
        }
        settings.build();
        Logger.info("Elasticsearch : Settings  " + settings.keys().toString());
        return settings;
    }

    public void stop() throws Exception {
        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }
}