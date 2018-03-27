/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intrence.datapipeline.tailor.task.RuleRecurrence;
import com.intrence.datapipeline.tailor.task.TaskRule;
import com.intrence.models.model.SearchParams;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskRuleMapper implements ResultSetMapper<TaskRule> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TaskRule map(int index, ResultSet resultSet, StatementContext ctx) throws SQLException {
        return new TaskRule.Builder()
                .id(resultSet.getInt(TaskRule.ID))
                .source(resultSet.getString(TaskRule.SOURCE))
                .type(resultSet.getString(TaskRule.TYPE))
                .maxRecords(resultSet.getInt(TaskRule.MAX_RECORDS))
                .status(TaskRule.Status.fromString(resultSet.getString(TaskRule.STATUS)))
                .searchParamsMap(convertSearchParams(resultSet.getString(TaskRule.SEARCH_PARAMS)))
                .recurrence(convertRuleRecurrence(resultSet.getString(TaskRule.RECURRENCE)))
                .clientId(resultSet.getString(TaskRule.CLIENT_ID))
                .taskBucket(resultSet.getString(TaskRule.TASK_BUCKET))
                .country(resultSet.getString(TaskRule.COUNTRY))
                .autoStart(resultSet.getBoolean(TaskRule.AUTO_START))
                .createdAt(resultSet.getTimestamp(TaskRule.CREATED_AT))
                .updatedAt(resultSet.getTimestamp(TaskRule.UPDATED_AT))
                .nextScheduleAt(resultSet.getTimestamp(TaskRule.NEXT_SCHEDULE_AT))
                .build();
    }

    private SearchParams convertSearchParams(String searchParamsJson) {
        if (searchParamsJson == null) {
            return null;
        }
        try {
            return objectMapper.readValue(searchParamsJson, SearchParams.class);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to convert SearchParams", ioe);
        }
    }

    private RuleRecurrence convertRuleRecurrence(String ruleRecurrenceJson) {
        if (ruleRecurrenceJson == null) {
            return null;
        }
        try {
            return objectMapper.readValue(ruleRecurrenceJson, RuleRecurrence.class);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to convert RuleRecurrence", ioe);
        }
    }
}
