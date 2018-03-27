/**
 * Created by wliu on 12/11/17.
 */
package com.intrence.datapipeline.tailor.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.intrence.core.persistence.jdbi.JDBI;
import com.intrence.datapipeline.tailor.TailorConfiguration;
import com.intrence.datapipeline.tailor.persistence.TaskRuleDao;
import com.intrence.datapipeline.tailor.persistence.TaskRuleMapper;
import com.intrence.datapipeline.tailor.persistence.TaskRunDao;
import com.intrence.datapipeline.tailor.persistence.TaskRunMapper;
import io.dropwizard.jdbi.args.JodaDateTimeArgumentFactory;
import io.dropwizard.jdbi.args.JodaDateTimeMapper;
import io.dropwizard.setup.Environment;
import org.skife.jdbi.v2.DBI;

import javax.sql.DataSource;

public class TaskModule extends AbstractModule {

    private final TailorConfiguration configuration;
    private final Environment environment;

    public TaskModule(TailorConfiguration configuration, Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        bind(TaskRunMapper.class).asEagerSingleton();
        bind(TaskRuleMapper.class).asEagerSingleton();
    }

    @Singleton
    @Provides
    public TaskRunDao providesTaskRunDao(JodaDateTimeArgumentFactory jodaDateTimeArgumentFactory, JodaDateTimeMapper jodaDateTimeMapper, TaskRunMapper taskRunMapper) {
        return buildTaskRunDao(configuration.getPostgresConfig().buildTransactionPooledDataSource(), jodaDateTimeArgumentFactory, jodaDateTimeMapper, taskRunMapper);
    }

    private TaskRunDao buildTaskRunDao(DataSource dataSource, JodaDateTimeArgumentFactory jodaDateTimeArgumentFactory, JodaDateTimeMapper jodaDateTimeMapper, TaskRunMapper taskRunMapper) {
        DBI dbi = JDBI.build(dataSource, environment.metrics());
        dbi.registerArgumentFactory(jodaDateTimeArgumentFactory);
        dbi.registerColumnMapper(jodaDateTimeMapper);
        dbi.registerMapper(taskRunMapper);
        return dbi.onDemand(TaskRunDao.class);
    }

    @Singleton
    @Provides
    public TaskRuleDao providesTaskRuleDao(JodaDateTimeArgumentFactory jodaDateTimeArgumentFactory, JodaDateTimeMapper jodaDateTimeMapper, TaskRuleMapper taskRuleMapper) {
        return buildTaskRuleDao(configuration.getPostgresConfig().buildTransactionPooledDataSource(), jodaDateTimeArgumentFactory, jodaDateTimeMapper, taskRuleMapper);
    }

    private TaskRuleDao buildTaskRuleDao(DataSource dataSource, JodaDateTimeArgumentFactory jodaDateTimeArgumentFactory, JodaDateTimeMapper jodaDateTimeMapper, TaskRuleMapper taskRuleMapper) {
        DBI dbi = JDBI.build(dataSource, environment.metrics());
        dbi.registerArgumentFactory(jodaDateTimeArgumentFactory);
        dbi.registerColumnMapper(jodaDateTimeMapper);
        dbi.registerMapper(taskRuleMapper);
        return dbi.onDemand(TaskRuleDao.class);
    }
}
