/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
