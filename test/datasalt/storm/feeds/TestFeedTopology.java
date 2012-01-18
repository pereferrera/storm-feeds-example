package datasalt.storm.feeds;

import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public class TestFeedTopology {

	@Test
	public void test() {
		/*
		 * Run the {@link FeedTopology} in local mode during 30 seconds
		 */
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, FeedTopology.buildTopology( Constants.FEEDS ));
		Utils.sleep(30000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
