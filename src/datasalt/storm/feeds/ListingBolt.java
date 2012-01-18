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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mortbay.log.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This class is a very simple example of real-time statistics computation. It takes the listings parsed from
 * {@link FetcherBolt} instances and calculates the last 10 listings emitted according to the listing date.
 * 
 * @author pere
 * 
 */
@SuppressWarnings("rawtypes")
public class ListingBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	List<Tuple> listings = new ArrayList<Tuple>();
	Set<String> listingIds = new HashSet<String>();

	int listSize = 10;

	OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// A thread will periodically report the last 10 listings
		Thread reporter = new Thread() {
			public void run() {
				while(true) {
					Log.info("Reporting start.");
					synchronized(listings) {
						for(Tuple tuple : listings) {
							ListingBolt.this.collector.emit(new Values(tuple.getStringByField("link"), tuple.getLongByField("date"),
							    tuple.getStringByField("description")));
							Log.info("Reporting listing URL: " + tuple.getStringByField("link") + " listing date: ["
							    + new Date(tuple.getLongByField("date")) + "]");
						}
					}
					Log.info("Reporting finished.");
					Utils.sleep(1000);
				}
			};
		};
		reporter.start(); // we don't care about properly finalizing this thread in this toy example...
	}

	@Override
	public void execute(Tuple input) {
		String listingId = input.getStringByField("link");
		synchronized(listings) {
			if(!listingIds.contains(listingId)) {
				listings.add(input);
				listingIds.add(listingId);
				if(listings.size() > listSize) {
					// Sort listings by date
					Collections.sort(listings, new Comparator<Tuple>() {
						@Override
						public int compare(Tuple t1, Tuple t2) {
							return new Long(t1.getLongByField("date")).compareTo(t2.getLongByField("date"));
						}
					});
					// Remove oldest one
					listings.remove(0);
				}
			}
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("link", "date", "description"));
	}
}
