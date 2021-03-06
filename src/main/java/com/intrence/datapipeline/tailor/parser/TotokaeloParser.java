/**
 * Created by wliu on 11/13/17.
 */
package com.intrence.datapipeline.tailor.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.datapipeline.tailor.extractor.HtmlExtractor;
import com.intrence.datapipeline.tailor.net.FetchRequest;
import org.htmlcleaner.TagNode;

public class TotokaeloParser extends ProductParser<TagNode> {

    public TotokaeloParser(String source, JsonNode rules, FetchRequest req, String content) {
        super(source, rules, req, new HtmlExtractor(source, content, req.getWorkRequest()));
    }
}
