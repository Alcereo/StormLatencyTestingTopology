package ru.alcereo;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class InfluxDBSender {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSender.class);

    // Configurations keys
    public static final String KEY_INFLUXDB_HOST = "metrics.influxdb.host";
    public static final String KEY_INFLUXDB_PORT = "metrics.influxdb.port";

    // Default config values for non requires
    public static final String DEFAULT_INFLUXDB_HOST = "localhost";
    public static final Integer DEFAULT_INFLUXDB_PORT = 8094;

    private final Integer influxdbPort;
    private final String influxdbHost;

    private List<Point> batchPoints;


    // Flags
    private Map<String, Object> fields;
    private Map<String, String> tags;

    public InfluxDBSender(Map<Object, Object> config) {

        LOG.debug("{}: config = {}", this.getClass().getSimpleName(), config.toString());

        this.influxdbHost = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_HOST, DEFAULT_INFLUXDB_HOST);
        this.influxdbPort = Integer.valueOf(getKeyValueOrDefaultValue(config, KEY_INFLUXDB_PORT, DEFAULT_INFLUXDB_PORT).toString());

    }

    /**
     * Look at the object collection if key exist, if not, it return defaultValue
     *
     * @param objects      Collection of Object
     * @param key          Key to lookup at objects collections
     * @param defaultValue default value to be returned
     * @return Object
     */
    private Object getKeyValueOrDefaultValue(Map<Object, Object> objects, String key, Object defaultValue) {
        if (objects.containsKey(key)) {
            return objects.get(key);
        } else {
            LOG.warn("{}: Using default parameter for {}", this.getClass().getSimpleName(), key);
            return defaultValue;
        }
    }

    /**
     * Prepare InfluxDB dataPoint to be send
     *
     * @param name  dataPoint name
     * @param value dataPoint value
     */
    public void prepareDataPoint(String name, String field, Object value) {

        if (this.batchPoints == null) {
            this.batchPoints = new ArrayList<>();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}: DataPoint name={} has value type={}", this.getClass().getSimpleName(), name, value.getClass().getName());
        }

        name = name.replaceAll("^[_]*", "");
        field = field.replaceAll("^[_]*", "");

        if (value instanceof String) {
            Point point = Point.measurement(name)
                    .addField(field, (String)value)
//                    .fields(this.fields)
                    .tag(this.tags)
                    .build();
            this.batchPoints.add(point);
        }else if (value instanceof Float
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double
                || value instanceof Number) {
            Point point = Point.measurement(name)
                    .addField(field, (Number)value)
//                    .fields(this.fields)
                    .tag(this.tags)
                    .build();
            this.batchPoints.add(point);
        }else if (value instanceof Boolean) {
            Point point = Point.measurement(name)
                    .addField(field, (Boolean)value)
//                    .fields(this.fields)
                    .tag(this.tags)
                    .build();
            this.batchPoints.add(point);

        } else {
            LOG.warn("{}: Unable to parse the Java type of 'value' : [type:'{}' value:'{}']",
                    this.getClass().getSimpleName(),
                    name,
                    value.getClass().getSimpleName()
            );
        }
    }

    /**
     * Send Points to InfluxDB server
     */
    public void sendPoints() throws IOException {

        if (this.batchPoints != null) {

            LOG.debug("{}: Sending points to database", this.getClass().getSimpleName());

            final DatagramChannel channel = DatagramChannel.open();

            try {
                InetSocketAddress socketAddress =
                        new InetSocketAddress(
                                this.influxdbHost,
                                this.influxdbPort
                        );

                this.batchPoints.forEach(point -> {
                    try {
                        channel.send(
                                ByteBuffer.wrap(point.lineProtocol().getBytes()),
                                socketAddress
                        );
                    } catch (IOException e) {
                        LOG.warn("Error to send metric to InfluxDB", e);
                    }
                });
            } finally {
                if (channel != null) {
                    channel.close();
                }
            }

            this.batchPoints = null;
        } else {
            LOG.warn("No points values to send");
        }
    }


    /**
     * Assign the field for every dataPoint.
     *
     * @param fields
     */
    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    /**
     * Assign the tags for every dataPoint.
     *
     * @param tags
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}