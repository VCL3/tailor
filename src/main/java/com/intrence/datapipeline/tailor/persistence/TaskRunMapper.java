/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.intrence.datapipeline.tailor.task.TaskRun;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskRunMapper implements ResultSetMapper<TaskRun> {

    @Override
    public TaskRun map(int index, ResultSet resultSet, StatementContext ctx) throws SQLException {
        return new TaskRun.Builder()
                .id(resultSet.getInt(TaskRun.ID))
                .taskRuleId(resultSet.getInt(TaskRun.TASK_RULE_ID))
                .status(resultSet.getString(TaskRun.STATUS))
                .stateSnapshot(resultSet.getString(TaskRun.STATE_SNAPSHOT))
                .count(resultSet.getInt(TaskRun.COUNT))
                .published_count(resultSet.getInt(TaskRun.PUBLISHED_COUNT))
                .createdAt(resultSet.getTimestamp(TaskRun.CREATED_AT))
                .updatedAt(resultSet.getTimestamp(TaskRun.UPDATED_AT))
                .build();
    }

}
