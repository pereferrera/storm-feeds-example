package datasalt.storm.feeds;

import java.net.URL;
import java.util.Date;
import java.util.Map;

import org.rometools.fetcher.FeedFetcher;
import org.rometools.fetcher.impl.HttpURLFeedFetcher;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;

/**
 * The FetcherBolt extends IRichBolt and implements fetching and parsings of feeds. It receives work (feed URLS)
 * shuffled from {@link FeedSpout}.
 * 
 * @author pere
 * 
 */
@SuppressWarnings("rawtypes")
public class FetcherBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private RegexBoilerplateRemoval bRemoval;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		bRemoval = new RegexBoilerplateRemoval();
	}

	@Override
	public void execute(Tuple input) {
		FeedFetcher feedFetcher = new HttpURLFeedFetcher();
		String feedUrl = input.getStringByField("feed");
		try {
			SyndFeed feed = feedFetcher.retrieveFeed(new URL(feedUrl));
			for(Object obj : feed.getEntries()) {
				SyndEntry syndEntry = (SyndEntry) obj;
				Date entryDate = getDate(syndEntry, feed);
				collector.emit(new Values(syndEntry.getLink(), entryDate.getTime(), bRemoval.removeBoilerplate(syndEntry
				    .getDescription().getValue())));
			}
			collector.ack(input);
		} catch(Throwable t) {
			t.printStackTrace();
			collector.fail(input);
		}
	}

	private Date getDate(SyndEntry syndEntry, SyndFeed feed) {
		return syndEntry.getUpdatedDate() == null ? (syndEntry.getPublishedDate() == null ? feed.getPublishedDate()
		    : syndEntry.getPublishedDate()) : syndEntry.getUpdatedDate();
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("link", "date", "description"));
	}
}
