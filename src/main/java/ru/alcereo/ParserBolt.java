package ru.alcereo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by alcereo on 23.09.17.
 */
public class ParserBolt extends BaseRichBolt {


    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        LatencySimulator.sleep(15);

        collector.emit(new Values(
                input.getValueByField("id"),
                input.getStringByField("name"),
                input.getStringByField("desc")
        ));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","name","desc"));
    }
}
