package datasalt.storm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * This class just overrides some methods from IRichSpout so that you don't need to override them if you extend it.
 * 
 * @author pere
 *
 */
@SuppressWarnings("rawtypes")
public class SimpleSpout implements IRichSpout {

  private static final long serialVersionUID = 1L;
  
	protected SpoutOutputCollector collector;
  Map conf;
	TopologyContext context;
	 
	@Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	  this.collector = collector;
	  this.conf = conf;
	  this.context = context;
  }

	@Override
  public void close() {
  }

	@Override
  public void nextTuple() {
  }

	@Override
  public void ack(Object msgId) {
  }

	@Override
  public void fail(Object msgId) {
  }

	@Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  
  }

	@Override
  public boolean isDistributed() {
	  return true;
  }
}
