package ru.alcereo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alcereo on 10.02.17.
 */
public class NameCountingBolt extends BaseRichBolt {

    private static final Logger log = LoggerFactory.getLogger(NameCountingBolt.class);

    private Map<String,Integer> counter = new HashMap<>();
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

//        MDC.put("name", input.getStringByField("name"));
//        log.debug("get new name");

        counter.put(
                input.getStringByField("name"),
                counter.get(input.getStringByField("name"))==null? 1: counter.get(input.getStringByField("name"))+1
        );

        input.getValueByField("id");
        input.getValueByField("name");
        input.getValueByField("desc");
//        input.getValueByField("rnd");
//        input.getValueByField("rnd2");

        LatencySimulator.sleep(5);

        collector.ack(input);

//        System.out.print("\r");
//        counter.forEach(
//                (s, integer) -> System.out.printf("%s - %d |",s,integer)
//        );

//        log.debug("Counter context: {}", collector);
//
//        MDC.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
