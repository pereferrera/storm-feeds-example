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
package datasalt.storm.feeds;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import datasalt.storm.SimpleSpout;

/**
 * The feed Spout extends {@link SimpleSpout} and emits Feed URLs to be fetched by {@link FetcherBolt} instances.
 * 
 * @author pere
 * 
 */
@SuppressWarnings("rawtypes")
public class FeedSpout extends SimpleSpout {

	private static final long serialVersionUID = 1L;
	Queue<String> feedQueue = new LinkedList<String>();
	String[] feeds;

	public FeedSpout(String[] feeds) {
		this.feeds = feeds;
	}

	@Override
	public void nextTuple() {
		String nextFeed = feedQueue.poll();
		if(nextFeed != null) {
			collector.emit(new Values(nextFeed), nextFeed);
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		for(String feed : feeds) {
			feedQueue.add(feed);
		}
	}

	@Override
	public void ack(Object feedId) {
		feedQueue.add((String) feedId);
	}

	@Override
	public void fail(Object feedId) {
		feedQueue.add((String) feedId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("feed"));
	}
}
