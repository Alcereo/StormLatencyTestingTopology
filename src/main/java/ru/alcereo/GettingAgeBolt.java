package ru.alcereo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by alcereo on 23.09.17.
 */
public class GettingAgeBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(GettingAgeBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
//        MDC.put("id", input.getValueByField("id"));
//        MDC.put("name", input.getStringByField("name"));
//        MDC.put("desc", input.getStringByField("desc"));

//        log.debug("get tuple, executing getting age");

        LatencySimulator.sleep(30);

        collector.emit(new Values(
                input.getValueByField("id"),
                input.getStringByField("name"),
                input.getStringByField("desc"),
                new Random().nextInt(99)
        ));

        collector.ack(input);

//        MDC.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","name","desc","rnd2"));
    }
}
