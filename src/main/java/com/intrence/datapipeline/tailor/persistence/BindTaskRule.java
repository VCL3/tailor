/**
 * Created by wliu on 12/5/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.intrence.datapipeline.tailor.task.TaskRule;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;

import java.lang.annotation.*;

@BindingAnnotation(BindTaskRule.TaskRuleBinderFactory.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface BindTaskRule {
    class TaskRuleBinderFactory implements BinderFactory {
        @Override
        public Binder build(Annotation annotation) {
            return (Binder<BindTaskRule, TaskRule>)
                    (q, bind, taskRule) -> {
                        q.bind("id", taskRule.getId());
                        q.bind("status", taskRule.getStatus());
                        q.bind("search_params", taskRule.getSearchParams());
                        q.bind("recurrence", taskRule.getRecurrence());
                        q.bind("source", taskRule.getSource());
                        q.bind("type", taskRule.getType());
                        q.bind("max_records", taskRule.getMaxRecords());
                        q.bind("client_id", taskRule.getClientId());
                        q.bind("task_bucket", taskRule.getTaskBucket());
                        q.bind("country", taskRule.getCountry());
                        q.bind("auto_start", taskRule.getAutoStart());
                        q.bind("created_at", taskRule.getCreatedAt());
                        q.bind("updated_at", taskRule.getUpdatedAt());
                        q.bind("next_schedule_at", taskRule.getNextScheduleAt());
                    };
        }
    }
}
