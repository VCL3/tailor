/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.intrence.datapipeline.tailor.task.TaskRun;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface TaskRunDao {

    @SqlUpdate("INSERT INTO task_runs (task_rule_id, count, published_count, created_at, updated_at) VALUES (:task_rule_id, :count, :published_count, :created_at, :updated_at)")
    @GetGeneratedKeys
    Integer createTaskRun(@Bind("task_rule_id") Integer taskRuleId, @Bind("count") Integer count, @Bind("published_count") Integer publishedCount, @Bind("created_at") DateTime createdAt, @Bind("updated_at") DateTime updatedAt);

    @SqlQuery("SELECT * FROM task_runs WHERE id = :id")
    TaskRun getTaskRunById(@Bind("id") Integer id);

    @SqlUpdate("UPDATE task_runs SET status = :status WHERE id = :id")
    void updateTaskRunStatusById(@Bind("id") Integer id, @Bind("status") String status);

    @SqlUpdate("UPDATE task_runs SET updated_at = :updated_at WHERE id = :id")
    void updateTaskRunUpdatedAtById(@Bind("id") Integer id, @Bind("updated_at") DateTime updatedAt);

    @SqlUpdate("UPDATE task_runs SET count = :count, published_count = :published_count WHERE id = :id")
    void updateTaskRunCountAndPublishedCountById(@Bind("id") Integer id, @Bind("count") Integer count, @Bind("published_count") Integer publishedCount);
}
