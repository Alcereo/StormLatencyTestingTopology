package ru.alcereo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by alcereo on 09.02.17.
 */
public class SimpleSpout extends BaseRichSpout {

    private static final Logger log = LoggerFactory.getLogger(SimpleSpout.class);

    private SpoutOutputCollector collector;
    private Random rand = new Random();
    private List<String> names = new ArrayList<>();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        names.add("Almared");
        names.add("Ivan");
        names.add("Andrey");
        names.add("Maxim");
        names.add("Igor");
        names.add("Sergey");
        names.add("Stupid");

    }

    public void nextTuple() {

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String name = names.get(rand.nextInt(names.size()-1));

//        MDC.put("name", name);
//        log.debug("commit name");

        collector.emit(
                new Values(UUID.randomUUID(),name ,"commited from: "+name),
                UUID.randomUUID()
        );

//        MDC.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","name","desc"));
//        System.out.println("declare fields spout");
    }
}
