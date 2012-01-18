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

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * This class builds the topology that needs to be submitted to Storm. It puts {@link FeedSpout}, {@link FetcherBolt}
 * and {@link ListingBolt} all together.
 * 
 * @author pere
 * 
 */
public class FeedTopology {

	public static StormTopology buildTopology(String[] feeds) {
		TopologyBuilder builder = new TopologyBuilder();

		// One single feed spout feeding data
		builder.setSpout("feedSpout", new FeedSpout(feeds), 1);

		// Various (2) fetcher bolts -> shuffle grouping from feed spout
		builder.setBolt("fetcherBolt", new FetcherBolt(), 2).shuffleGrouping("feedSpout");
		// One single listing bolt calculating statistics
		builder.setBolt("listingBolt", new ListingBolt(), 1).globalGrouping("fetcherBolt");

		return builder.createTopology();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(1);
		StormSubmitter.submitTopology("feedTopology", conf, buildTopology(Constants.FEEDS));
	}
}
