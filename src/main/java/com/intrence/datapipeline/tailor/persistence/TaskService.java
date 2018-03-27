/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.persistence;

import com.google.inject.Inject;
import com.intrence.datapipeline.tailor.task.TaskRule;
import com.intrence.datapipeline.tailor.task.TaskRun;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.sqlobject.Transaction;

public class TaskService {

    private final TaskRunDao taskRunDao;
    private final TaskRuleDao taskRuleDao;

    @Inject
    public TaskService(TaskRunDao taskRunDao, TaskRuleDao taskRuleDao) {
        this.taskRunDao = taskRunDao;
        this.taskRuleDao = taskRuleDao;
    }

    // TaskRun
    public Integer createTaskRun(TaskRule taskRule) {
        return taskRunDao.createTaskRun(taskRule.getId(), 0, 0, DateTime.now(), DateTime.now());
    }

    public TaskRun getTaskRunById(Integer id) {
        return taskRunDao.getTaskRunById(id);
    }

    @Transaction
    public TaskRun createAndReturnTaskRun(TaskRule taskRule) {
        Integer taskRunId = createTaskRun(taskRule);
        return getTaskRunById(taskRunId);
    }

    public void updateTaskRunStatusById(Integer taskId, String status) {
        taskRunDao.updateTaskRunStatusById(taskId, status);
    }

    public void updateTaskRunUpdatedAtById(Integer taskId, DateTime updatedAt) {
        taskRunDao.updateTaskRunUpdatedAtById(taskId, updatedAt);
    }

    public void updateTaskRunCountAndPublishedCountById(Integer id, Integer count, Integer publishedCount) {
        taskRunDao.updateTaskRunCountAndPublishedCountById(id, count, publishedCount);
    }

    // TaskRule
    public Integer createTaskRule(TaskRule taskRule) {
        return taskRuleDao.createTaskRule(taskRule);
    }

    public TaskRule getTaskRuleById(Integer id) {
        return taskRuleDao.getTaskRuleById(id);
    }

    public Integer getUrlsFetchedCountFromTaskRuleById(Integer id) {
        return taskRuleDao.getUrlsFetchedCountFromTaskRuleById(id);
    }

    public void updateTaskRule(TaskRule taskRule) {
        taskRuleDao.updateTaskRule(taskRule);
    }

    public void updateTaskRuleUrlsFetchedCountAndUpdatedAtById(Integer id, Integer urlsFetchedCount, DateTime updatedAt) {
        taskRuleDao.updateTaskRuleUrlsFetchedCountAndUpdatedAtById(id, urlsFetchedCount, updatedAt);
    }

    public void updateTaskRuleStatusAndUpdatedAtById(Integer id, String status, DateTime updatedAt) {
        taskRuleDao.updateTaskRuleStatusAndUpdatedAtById(id, status, updatedAt);
    }

    public void markTaskRuleAsFinished(Integer id) {
        taskRuleDao.updateTaskRuleStatusAndUpdatedAtById(id, TaskRule.Status.FINISHED.toString(), DateTime.now());
    }

}
