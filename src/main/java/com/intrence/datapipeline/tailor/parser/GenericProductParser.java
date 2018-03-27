package com.intrence.datapipeline.tailor.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.datapipeline.tailor.extractor.HtmlExtractor;
import com.intrence.datapipeline.tailor.net.FetchRequest;
import org.htmlcleaner.TagNode;

public class GenericProductParser extends ProductParser<TagNode> {

    public GenericProductParser(String source, JsonNode rules, FetchRequest req, String content) {
        super(source, rules, req, new HtmlExtractor(source, content, req.getWorkRequest()));
    }

}
