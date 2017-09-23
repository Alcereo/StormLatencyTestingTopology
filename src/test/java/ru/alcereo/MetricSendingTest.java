package ru.alcereo;

import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Map;

/**
 * Created by alcereo on 22.09.17.
 */
public class MetricSendingTest {

    @Test
    public void SendMetric(){

        Map<Object, Object> influxDbConfig = new HashedMap();

        influxDbConfig.put(InfluxDBSender.KEY_INFLUXDB_HOST, "172.55.0.5");
        influxDbConfig.put(InfluxDBSender.KEY_INFLUXDB_PORT, 8094);

        InfluxDBSender influxDBSender = new InfluxDBSender(influxDbConfig);

//        influxDBSender.prepareConnection();

        influxDBSender.setFields(Maps.asMap(Sets.newHashSet("field1","field2"), s -> s));
        influxDBSender.setTags(Maps.asMap(Sets.newHashSet("tag1","tag2"), s -> s));
        influxDBSender.prepareDataPoint("test","value",3);

        try {
            influxDBSender.sendPoints();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void InfluxDbUDPSend() throws IOException {

        DatagramChannel channel = null;

        try {
            InetSocketAddress socketAddress = new InetSocketAddress("172.55.0.5", 8094);

            String json = "cpu_load_short,host=server02 str=\"sdfdfgsh\"";
            String json1 = "cpu_load_short,host=server02 some=3";
            String json2 = "cpu_load_short,host=server02 value=5";


            Point p1 = Point.measurement("cpu")
                    .addField("val", 3)
                    .build();

            Point p2 = Point.measurement("cpu")
                    .addField("val", 3)
                    .addField("some", 4)
                    .build();

            ByteBuffer buffer = ByteBuffer.wrap(p1.lineProtocol().getBytes());
            channel = DatagramChannel.open();
            channel.send(buffer, socketAddress);
//            buffer.clear();

//            channel.close();

//            channel = DatagramChannel.open();
            buffer = ByteBuffer.wrap(p2.lineProtocol().getBytes());
            channel.send(buffer, socketAddress);
//            buffer.clear();
//
//
//           channel = DatagramChannel.open();
            buffer = ByteBuffer.wrap(json2.getBytes());
            channel.send(buffer, socketAddress);
//            buffer.clear();

        } catch (Exception e) {
            throw e;
        } finally {
            if (channel != null) {
                channel.close();
            }
        }

    }


    @Test
    public void lineGeneration(){

        BatchPoints points = BatchPoints.database("test").build();

        points.point(
                Point.measurement("some")
                .addField("val", 2.234000000000)
                        .tag("some","hetre")
                .build()
        );

        points.point(
                Point.measurement("some")
                        .addField("val",3)
                        .addField("some",4)
                        .addField("ewafd",6)
                        .addField("fff",5)
                        .build()
        );

        System.out.println(points.lineProtocol());

        System.out.println(Point.measurement("some")
                .addField("val", 2.234000000000)
                .tag("some","hetre")
                .build().lineProtocol());
    }

}
