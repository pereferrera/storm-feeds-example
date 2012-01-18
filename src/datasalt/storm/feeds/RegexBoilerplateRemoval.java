package datasalt.storm.feeds;

import java.util.regex.Pattern;

/**
 * This is a very simple regex-based method for removing "boilerplate" (e.g. HTML tags, javascript...).
 * 
 * @author pere
 * 
 */
public class RegexBoilerplateRemoval {

	Pattern scripts = Pattern.compile(Pattern.quote("<script") + "[ ]*[^>]*>" + ".*?" + Pattern.quote("</script>"),
	    Pattern.DOTALL);
	Pattern noScripts = Pattern.compile(Pattern.quote("<noscript") + "[ ]*[^>]*>" + ".*?" + Pattern.quote("</noscript>"),
	    Pattern.DOTALL);
	Pattern styles = Pattern.compile(Pattern.quote("<style") + "[ ]*[^>]*>" + ".*?" + Pattern.quote("</style>"),
	    Pattern.DOTALL);
	Pattern myRegex = Pattern.compile("\\<.*?>", Pattern.DOTALL);
	Pattern manySpaces = Pattern.compile("\\p{Space}+");

	public String removeBoilerplate(String str) {
		if(str == null) {
			return null;
		}
		return manySpaces
		    .matcher(
		        myRegex
		            .matcher(
		                styles.matcher(noScripts.matcher(scripts.matcher(str).replaceAll(" ")).replaceAll(" ")).replaceAll(
		                    " ")).replaceAll(" ")).replaceAll(" ").trim();
	}
}
