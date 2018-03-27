/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.intrence.datapipeline.tailor.task.TaskRule;
import com.intrence.datapipeline.tailor.util.Constants;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface TaskRuleDao {

    @SqlUpdate("INSERT INTO task_rules (status, search_params, recurrence, source, type, max_records, client_id, task_bucket, country, auto_start, created_at, updated_at, next_schedule_at) VALUES (:status, :search_params, :recurrence, :source, :type, :max_records, :client_id, :task_bucket, :country, :auto_start, :created_at, :updated_at, :next_schedule_at)")
    @GetGeneratedKeys
    Integer createTaskRule(@BindTaskRule TaskRule taskRule);

    @SqlQuery("SELECT * FROM task_rules WHERE id = :id")
    TaskRule getTaskRuleById(@Bind("id") Integer id);

    @SqlQuery("SELECT urls_fetched FROM task_rules WHERE id = :id")
    Integer getUrlsFetchedCountFromTaskRuleById(@Bind("id") Integer id);

//    @SqlQuery(String.format("SELECT * FROM task_rules WHERE %s = (?) AND %s", TaskRule.SOURCE, getORPredicate(statuses, "status"))
//    TaskRule getTaskRuleBySourceAndStatus();

    @SqlUpdate("UPDATE task_rules SET (status, search_params, recurrence, source, type, max_records, client_id, task_bucket, country, auto_start, created_at, updated_at, next_schedule_at) = (:status, :search_params, :recurrence, :source, :type, :max_records, :client_id, :task_bucket, :country, :auto_start, :created_at, :updated_at, :next_schedule_at) WHERE id = :id")
    void updateTaskRule(@BindTaskRule TaskRule taskRule);

    @SqlUpdate("UPDATE task_rules SET (urls_fetched, updated_at) = (:urls_fetched, :updated_at) WHERE id = :id")
    void updateTaskRuleUrlsFetchedCountAndUpdatedAtById(@Bind("id") Integer id, @Bind("urls_fetched") Integer urlsFetchedCount, @Bind("updated_at") DateTime updatedAt);

    @SqlUpdate("UPDATE task_rules SET (status, updated_at) = (:status, :updated_at) WHERE id = :id")
    void updateTaskRuleStatusAndUpdatedAtById(@Bind("id") Integer id, @Bind("status") String status, @Bind("updated_at") DateTime updatedAt);
}
