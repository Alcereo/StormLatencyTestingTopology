package ru.alcereo;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by alcereo on 22.09.17.
 */
public class InfluxDBMetricsConsumer implements IMetricsConsumer{

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBMetricsConsumer.class);

    private InfluxDBSender influxDBSender;
    private String topologyName;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {

        final Map<Object, Object> mergedConf = new HashMap<>();

        if (stormConf != null && stormConf.size() > 0) {

            LOG.debug("{}: Argument stormConf: {}", this.getClass().getSimpleName(), stormConf.toString());

            this.topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
            mergedConf.putAll(stormConf);
        } else {
            LOG.warn("{}: Argument stormConf is Empty or null", this.getClass().getSimpleName());
        }

        if (registrationArgument != null && registrationArgument instanceof Map && ((Map) registrationArgument).size() > 0) {

            LOG.debug("{}: Argument registrationArgument: {}", this.getClass().getSimpleName(), registrationArgument.toString());

            mergedConf.putAll((Map) registrationArgument);
        } else {
            LOG.warn("{}: Argument registrationArgument is Empty or null", this.getClass().getSimpleName());
        }

        this.influxDBSender = makeInfluxDBSender(mergedConf);
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        // Necessary for the topology to continue working when InfluxDB is off-line
        try {

            @SuppressWarnings("unchecked") final Map<String, String> tags = new HashMap();

            // InfluxDB tags are like a field but indexed
            tags.put("component-id", taskInfo.srcComponentId.replaceAll("^[_]*", ""));
            tags.put("topology", this.topologyName);
            tags.put("task-id", String.valueOf(taskInfo.srcTaskId));
            tags.put("worker-host", taskInfo.srcWorkerHost);
            tags.put("worker-port", String.valueOf(taskInfo.srcWorkerPort));

            // InfluxDB fields per each data point
//            fields.put("Timestamp", String.valueOf(taskInfo.timestamp));
//            fields.put("UpdateIntervalSecs", String.valueOf(taskInfo.updateIntervalSecs));

            // sendPoints data to InfluxDB
            this.influxDBSender.setTags(tags);

            // Prepare data to be parse
            for (DataPoint dataPoint : dataPoints) {
                if (dataPoint.value != null) {
                    this.processDataPoint(dataPoint.name, dataPoint.value);
                } else {
                    LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), dataPoint.name);
                }
            }

            this.influxDBSender.sendPoints();

        } catch (Exception e) {
            LOG.warn("{}: The collected data will be lost. Exception = {}", this.getClass().getSimpleName(), e);
        }
    }

    /**
     * Verify if DataPoint type is a Map and decompose it using recursion,
     * then send it to influxDBSender.
     *
     * @param name           dataPoint name
     * @param value          dataPoint value
     */
    public void processDataPoint(String name, Object value) {

        if (value instanceof String
                || value instanceof Float
                || value instanceof Integer
                || value instanceof Boolean
                || value instanceof Long
                || value instanceof Double
                || value instanceof Number) {

            LOG.debug("{}: Processing dataPoint: [ name: '{}', value: '{}' ]", this.getClass().getSimpleName(), name, value);

            this.influxDBSender.prepareDataPoint(name, "value", value);
        } else if (value instanceof Map) {

            LOG.debug("{}: Processing dataPoint<Map> ...", this.getClass().getSimpleName());

            @SuppressWarnings("unchecked")
            Map<String, Object> values = (Map<String, Object>) value;

            for (Map.Entry<String, Object> entry : values.entrySet()) {

                LOG.debug("{}: ... Processing Map dataPoint entry: [ name: '{}', value: '{}' ]", this.getClass().getSimpleName(), entry.getKey(), entry.getValue());

                if (entry.getValue() != null) {

                    this.influxDBSender.prepareDataPoint(name, entry.getKey(), entry.getValue());
                } else {
                    LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), name);
                }
            }
        }else {
            LOG.warn("{}: Discarding dataPoint: {}, value is unreadable type", this.getClass().getSimpleName(), name);
        }
    }

    @Override
    public void cleanup() {
    }

    /**
     * Factory for InfluxDBSender
     *
     * @param config map
     * @return InfluxDBSender new Instance
     */
    InfluxDBSender makeInfluxDBSender(Map<Object, Object> config) {
        return new InfluxDBSender(config);
    }
}