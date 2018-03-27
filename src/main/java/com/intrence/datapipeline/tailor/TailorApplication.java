package com.intrence.datapipeline.tailor;

import com.google.inject.Injector;
import com.intrence.core.modules.ElasticSearchModule;
import com.intrence.core.modules.PostgresModule;
import com.intrence.datapipeline.tailor.health.TailorHealthCheck;
import com.intrence.datapipeline.tailor.job.ProductIngestionJob;
import com.intrence.datapipeline.tailor.module.TailorModule;
import com.intrence.datapipeline.tailor.module.TaskModule;
import com.intrence.datapipeline.tailor.resources.FetchProductResource;
import com.intrence.datapipeline.tailor.resources.IndexProductResource;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.Arrays;

public class TailorApplication extends Application<TailorConfiguration> {

    public static void main(final String[] args) throws Exception {
        new TailorApplication().run(args);
    }

    @Override
    public String getName() {
        return "tailor";
    }

    @Override
    public void initialize(final Bootstrap<TailorConfiguration> bootstrap) {
        // TODO: application initialization
    }

    @Override
    public void run(final TailorConfiguration configuration, final Environment environment) throws Exception {
        // Governator Guice Injector
        Injector injector = LifecycleInjector.builder().withModules(
                Arrays.asList(
                        new PostgresModule(),
                        new ElasticSearchModule(configuration.getElasticSearchConfig(), environment),
                        new TailorModule(configuration, environment),
                        new TaskModule(configuration, environment)))
                .build()
                .createInjector();
        environment.jersey().register(injector);

//        LifecycleManager lifecycleManager = injector.getInstance(LifecycleManager.class);
//        lifecycleManager.start();

        // Lifecycle Management
        LifecycleEnvironment lifecycleEnvironment = environment.lifecycle();

        // Add task and manage in lifecycle
        ProductIngestionJob productIngestionJob = injector.getInstance(ProductIngestionJob.class);
        environment.admin().addTask(productIngestionJob);
        lifecycleEnvironment.manage(productIngestionJob);

        // Register Resources
        environment.jersey().register(injector.getInstance(FetchProductResource.class));
        environment.jersey().register(injector.getInstance(IndexProductResource.class));
        environment.healthChecks().register("math", new TailorHealthCheck());
    }

}
