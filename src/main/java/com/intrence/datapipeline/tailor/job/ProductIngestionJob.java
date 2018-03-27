/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.job;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import com.intrence.core.elasticsearch.ElasticSearchService;
import com.intrence.core.persistence.dao.ProductDao;
import com.intrence.datapipeline.tailor.net.WebFetcher;
import com.intrence.datapipeline.tailor.persistence.TaskService;
import com.intrence.datapipeline.tailor.queue.FetchRequestQueue;
import com.intrence.datapipeline.tailor.task.ProductTask;
import com.intrence.datapipeline.tailor.task.TaskRule;
import com.intrence.datapipeline.tailor.util.Constants;
import com.intrence.datapipeline.tailor.util.JobUtil;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.UUID;

public class ProductIngestionJob extends Task implements Managed {

    private static final String NAME = "ProductIngestionJob";

    private final TaskService taskService;
    private final FetchRequestQueue fetchRequestQueue;
    private final WebFetcher webFetcher;
    private final ProductDao productDao;
    private final ElasticSearchService elasticSearchService;

    @Inject
    public ProductIngestionJob(TaskService taskService, FetchRequestQueue fetchRequestQueue, WebFetcher webFetcher, ProductDao productDao, ElasticSearchService elasticSearchService) {
        super(NAME);
        this.taskService = taskService;
        this.fetchRequestQueue = fetchRequestQueue;
        this.webFetcher = webFetcher;
        this.productDao = productDao;
        this.elasticSearchService = elasticSearchService;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        UUID sessionId = UUID.randomUUID();

        // Build TaskRule based on parameters
        TaskRule taskRule = buildTaskRule(parameters);

        if (taskRule != null) {
            Integer taskRuleId = this.taskService.createTaskRule(taskRule);
            taskRule.setTaskRuleId(taskRuleId);
        } else {
            output.write("Session " + sessionId.toString() + " TaskRule is null");
            return;
        }

        Integer parallelism = Integer.parseInt(parameters.get("parallelism").asList().get(0));

        ProductTask productTask = new ProductTask(taskRule, this.taskService, this.fetchRequestQueue, this.webFetcher, this.productDao, this.elasticSearchService, parallelism);
        productTask.start();

        output.write(String.format("{\"status\":\"Finished Job\", \"session_id\":\"%s\"}", sessionId));
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private TaskRule buildTaskRule(ImmutableMultimap<String, String> parameters) {

        String source = JobUtil.extractParameterFromParameters(Constants.SOURCE, parameters);
        String type = JobUtil.extractParameterFromParameters(Constants.TYPE, parameters);
        String clientId = JobUtil.extractParameterFromParameters(Constants.CLIENT_ID, parameters);
        Boolean autoStart = Boolean.parseBoolean(JobUtil.extractParameterFromParameters(Constants.AUTO_START, parameters));

        return new TaskRule.Builder()
                .type(type)
                .status(TaskRule.Status.RUNNABLE)
                .source(source)
                .taskBucket("")
                .country("US")
                .autoStart(autoStart)
                .clientId(clientId)
                .maxRecords(10)
                .searchParamsMap(null)
                .recurrence(null)
                .nextScheduleAt(new Timestamp(System.currentTimeMillis()))
                .build();
    }

}
