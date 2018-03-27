/**
 * Created by wliu on 12/12/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.codahale.metrics.MetricRegistry;
import com.groupon.jtier.daas.postgres.PostgresRule;
import com.intrence.core.persistence.jdbi.JDBI;
import com.intrence.datapipeline.tailor.task.TaskRule;
import com.intrence.datapipeline.tailor.task.TaskRun;
import io.dropwizard.jdbi.args.JodaDateTimeArgumentFactory;
import org.joda.time.DateTime;
import org.junit.*;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TaskServiceTest {

    private static Handle handle;
    private static TaskRunDao taskRunDao;
    private static TaskRuleDao taskRuleDao;
    private static Integer taskRuleId;
    private static Integer taskRunId;

    @ClassRule
    public static final PostgresRule postgres = PostgresRule.withSharedDatabase()
            .cleanBefore()
            .migrate()
            .cleanAfter();

    @BeforeClass
    public static void setUp() throws Exception {
        DBI dbi = JDBI.build(postgres.getTransactionDataSource(), new MetricRegistry());
        dbi.registerMapper(new TaskRunMapper());
        dbi.registerMapper(new TaskRuleMapper());
        dbi.registerArgumentFactory(new JodaDateTimeArgumentFactory());
        handle = dbi.open();
        taskRunDao = dbi.onDemand(TaskRunDao.class);
        taskRuleDao = dbi.onDemand(TaskRuleDao.class);
    }

    @Test
    public void testCreateTaskRule() throws Exception {
        TaskRule taskRule = new TaskRule.Builder()
                .clientId("test_client")
                .status(TaskRule.Status.FINISHED)
                .source("test_source")
                .type("test_type")
                .build();
        taskRuleId = taskRuleDao.createTaskRule(taskRule);
        TaskRule taskRuleFromDB = taskRuleDao.getTaskRuleById(taskRuleId);
        assertNotNull(taskRuleFromDB);
        assertEquals("test_client", taskRule.getClientId());
        assertEquals(TaskRule.Status.FINISHED, taskRule.getStatus());
    }

    @Test
    public void testCreateTaskRun() throws Exception {
        taskRunId = taskRunDao.createTaskRun(taskRuleId, 0, 0, DateTime.now(), DateTime.now());
        TaskRun taskRun = taskRunDao.getTaskRunById(taskRunId);
        assertNotNull(taskRun);
        assertEquals(0, taskRun.getCount());
    }

}
