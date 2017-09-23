package ru.alcereo;

import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static java.util.Arrays.asList;
import static ru.alcereo.StormRunner.runTopologyLocally;
import static ru.alcereo.StormRunner.runTopologyRemotely;

/**
 * Created by alcereo on 09.02.17.
 */
public class Main {

    private final static String TOPOLOGY_NAME = "simple-name-counting-topology";

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        System.out.println("Start configuring topology...");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("simple-spot", new SimpleSpout(), 5);


        builder.setBolt("parser-bolt", new ParserBolt(), 3)
                .setNumTasks(8)
                .shuffleGrouping("simple-spot");

        builder
                .setBolt("geting-name-bolt", new GettingNameBolt(),3)
                .setNumTasks(32)
                .shuffleGrouping("parser-bolt");

        builder
                .setBolt("geting-age-bolt", new GettingAgeBolt(), 3)
                .setNumTasks(32)
                .shuffleGrouping("parser-bolt");

        builder
                .setBolt("count-bolt", new NameCountingBolt(), 3)
                .setNumTasks(8)
                .fieldsGrouping("geting-name-bolt", new Fields("name"))
                .fieldsGrouping("geting-age-bolt", new Fields("name"));


        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);
        config.registerMetricsConsumer(InfluxDBMetricsConsumer.class);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);
        config.put(InfluxDBSender.KEY_INFLUXDB_HOST, "172.55.0.5");
        config.put(InfluxDBSender.KEY_INFLUXDB_PORT, 8094);
        config.put(Config.STORM_CLUSTER_METRICS_CONSUMER_PUBLISH_INTERVAL_SECS, 2);
        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 4);

        config.setMaxSpoutPending(500);

        config.put(Config.STORM_CLUSTER_METRICS_CONSUMER_PUBLISH_INTERVAL_SECS, 2);
        config.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 1);

        System.out.println("Finish configuring topology...");
        System.out.println("Start building and run topology...");

        if (asList(args).contains("local")) {
            runTopologyLocally(
                    builder.createTopology(),
                    TOPOLOGY_NAME,
                    config,
                    240
            );
        }else if (asList(args).contains("remote")){
            runTopologyRemotely(
                    builder.createTopology(),
                    TOPOLOGY_NAME,
                    config
            );
        }else{
            throw new IllegalArgumentException("Require argument: local or remote");
        }

        System.out.println("Finish building and run topology...");
        System.out.println("Shutdown...");

    }
}
