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
