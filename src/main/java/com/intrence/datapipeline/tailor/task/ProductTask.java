package com.intrence.datapipeline.tailor.task;

import com.google.inject.Inject;
import com.intrence.core.elasticsearch.ElasticSearchService;
import com.intrence.core.persistence.dao.ProductDao;
import com.intrence.datapipeline.tailor.exception.TailorBackendException;
import com.intrence.datapipeline.tailor.exception.ThresholdReachedException;
import com.intrence.datapipeline.tailor.parser.ProductParser;
import com.intrence.datapipeline.tailor.queue.FetchRequestQueue;
import com.intrence.datapipeline.tailor.net.FetchRequest;
import com.intrence.datapipeline.tailor.net.RequestResponse;
import com.intrence.datapipeline.tailor.net.WebFetcher;
import com.intrence.datapipeline.tailor.parser.BaseParser;
import com.intrence.datapipeline.tailor.parser.ParserFactory;
import com.intrence.datapipeline.tailor.persistence.TaskService;
import com.intrence.datapipeline.tailor.util.Constants;
import com.intrence.models.model.DataPoint;
import com.intrence.models.model.Product;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ProductTask extends Task {

    public static int K_PER_BATCH = 10;
    private final ProductDao productDao;
    private final ElasticSearchService elasticSearchService;

    @Inject
    public ProductTask(TaskRule rule,
                       TaskService taskService,
                       FetchRequestQueue fetchRequestQueue,
                       WebFetcher webFetcher,
                       ProductDao productDao,
                       ElasticSearchService elasticSearchService,
                       Integer parallelism) {
        super(rule, taskService, fetchRequestQueue, webFetcher, parallelism);
        this.productDao = productDao;
        this.elasticSearchService = elasticSearchService;
    }

    /**
     * Abstract Methods
     */
    @Override
    public int numberOfFetchReqsPerBatch() {
        return K_PER_BATCH;
    }

    @Override
    public Set<FetchRequest> processFetchRequests(Set<FetchRequest> fetchRequests) throws InterruptedException, TailorBackendException, ThresholdReachedException {

        Set<FetchRequest> failedRequests = new HashSet<>();

        for (FetchRequest request: fetchRequests) {
            if (Thread.currentThread().interrupted()) {
                infoLog("Task stop flag activated, throwing InterruptedException");                
                throw new InterruptedException();
            }

            try {

                long searchStart = System.currentTimeMillis();
                RequestResponse response;

                try {
                    infoLog(String.format("Fetching data from the url=%s",request.getWorkRequest()));

                    // TaskRule Type "stream"
                    if (Constants.STREAM_OPERATION.equals(this.rule.getType())) {
                        response = this.webFetcher.getStreamResponse(this.rule.getSource(), request);
                    } else {
                        response = this.webFetcher.getResponse(this.rule.getSource(), request);
                    }

                } catch (Exception ex) {
                    errorLog(String.format("Exception=WebFetcherException error while getting response for url=%s, " +
                            "time_taken=%d", request.getWorkRequest(), System.currentTimeMillis()-searchStart), ex);
                    failedRequests.add(request);
                    continue;
                }

                long searchEnd = System.currentTimeMillis();
                infoLog(String.format("Event=WebFetcherResponse received response for the url=%s, " +
                                "time_taken=%d, http_status=%d, redirected_url=%s", request.getWorkRequest(),
                        System.currentTimeMillis()-searchStart, response.getStatusCode(), response.getRedirectedUrl()));
                
                long parseTime = 0;
                int statusCode = response.getStatusCode();

                if (Constants.HTTP_STATUS_CODES_TO_RETRY.contains(statusCode)) {
                    failedRequests.add(request); 
                    continue;
                }

                // Successful Responses
                if (statusCode >= 200 && statusCode < 300) {
                    Set<FetchRequest> extractedReqs = handleSuccessfulRequests(response);
                    if (extractedReqs != null) {
                        scheduleExtractedRequests(extractedReqs);
                    }
                    parseTime = System.currentTimeMillis() - searchEnd;
                } else if (Constants.HTTP_REDIRECTION_STATUS_CODES.contains(response.getStatusCode())) {
                    if (response.getResponse() != null) {
                        FetchRequest redirectedReq = handleRedirectedRequests(response);
                        if (redirectedReq != null) {
                            Set<FetchRequest> reqs = new HashSet<>();
                            reqs.add(redirectedReq);
                            scheduleExtractedRequests(reqs);
                        }
                    }
                }

                long end = System.currentTimeMillis();
                infoLog(String.format("Event=FetchReqProcessing crawl_time=%s, place_parse_time=%s," +
                        "total_time=%s, url=%s ", searchEnd - searchStart, parseTime,
                        end - searchStart, request.getWorkRequest()));

            } catch (ThresholdReachedException ex) {
                throw ex;
            } catch (Exception ex){
                failedRequests.add(request);
                errorLog(String.format("Exception=FetchReqProcessing for the url=%s", request.getWorkRequest()),ex);
            }
        }
        return failedRequests;
    }

    
    protected void scheduleExtractedRequests(Set<FetchRequest> fetchRequests) throws TailorBackendException {
        this.fetchRequestQueue.addAll(this.rule.getId(), fetchRequests);
    }

    protected void persistRawData(RequestResponse requestResponse){
        JSONObject json = new JSONObject();
        json.put("data", requestResponse.getResponse());
        json.put("url", requestResponse.getRequest().getWorkRequest());
        super.persistRawData(json);
    }

    // TODO: Handle successful request
    protected Set<FetchRequest> handleSuccessfulRequests(RequestResponse response) throws Exception {

        // Get the proper parser
        BaseParser parser = (BaseParser) ParserFactory.createParser(this.rule.getSource(), response.getResponse(), response.getRequest());

        Set<DataPoint> dataPoints = parser.extractEntities();
        if (dataPoints != null && dataPoints.size() > 0) {
            taskService.updateTaskRunCountAndPublishedCountById(run.getId(), dataPoints.size(), 1);
            for (DataPoint dataPoint : dataPoints) {
                Product product = dataPoint.getProduct();
                // Has product and product is currently on sale
                if (product != null && product.getIsOnSale()) {
                    productDao.createProduct(product);
                    elasticSearchService.upsertDocument("products", "doc", product.getUuid().toString(), product.toJson(), false);
                }
            }
        }
        return parser.extractLinks();
    }

    protected FetchRequest handleRedirectedRequests(RequestResponse response){
        // Check if we need to enqueue this redirected url for different api sources. e.g. factual
        infoLog(String.format("Event=RedirectionEvent  -  actualUrl=%s  -  redirectedUrl=%s",
                response.getRequest().getWorkRequest(), response.getRedirectedUrl()));
        return new FetchRequest(response.getRedirectedUrl(),1);
    }

}
