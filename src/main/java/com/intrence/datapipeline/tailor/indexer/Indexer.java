//package com.intrence.datapipeline.tailor.indexer;
//
//import com.codahale.metrics.annotation.Timed;
//import com.google.common.base.Optional;
//import com.google.common.eventbus.EventBus;
//import io.dropwizard.util.Duration;
//import org.apache.commons.lang3.tuple.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observable;
//import rx.functions.Action0;
//import rx.functions.Action1;
//import rx.functions.Func1;
//
//import javax.inject.Inject;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class Indexer {
//
//    private static final Logger log = LoggerFactory.getLogger(Indexer.class);
//
//    private final EventBus eventBus;
//    private final AbstractItemIdSource itemIdSource;
//    private final AbstractItemsAssembler<T> itemsAssembler;
//    private final ItemsSender<T> itemsSender;
//    private final Optional<LocationMappingClient> locationMappingClient;
//    private final SendersDeadItemUuidQueue sendersDeadItemUuidQueue;
//    private final ScheduleConfiguration scheduleConfiguration;
//    private final Optional<IndexRepository> indexRepositoryOptional;
//    private final AtomicInteger cycleNumber;
//
//    @Inject
//    Indexer(EventBus eventBus,
//            AbstractItemIdSource itemIdSource,
//            AbstractItemsAssembler<T> itemsAssembler,
//            ItemsSender<T> itemsSender,
//            Optional<LocationMappingClient> locationMappingClient,
//            SendersDeadItemUuidQueue sendersDeadItemUuidQueue,
//            ScheduleConfiguration scheduleConfiguration,
//            Optional<IndexRepository> indexRepositoryOptional) {
//        this.eventBus = eventBus;
//        this.itemIdSource = itemIdSource;
//        this.itemsAssembler = itemsAssembler;
//        this.itemsSender = itemsSender;
//        this.locationMappingClient = locationMappingClient;
//        this.sendersDeadItemUuidQueue = sendersDeadItemUuidQueue;
//        this.scheduleConfiguration = scheduleConfiguration;
//        this.indexRepositoryOptional = indexRepositoryOptional;
//        this.cycleNumber = new AtomicInteger(0);
//    }
//
//    @Timed
//    public void index() {
//        try {
//            log.info("Items indexing started");
//            eventBus.post(IndexingStartedEvent.getInstance());
//
//            if (locationMappingClient.isPresent() && cycleNumber.get() == 0) {
//                log.info("Pre-fetching city and neighborhood files started");
//                locationMappingClient.get().prefetch();
//                log.info("Pre-fetching city and neighborhood files completed");
//            }
//
//            itemsSender.send(
//                    itemsAssembler.assemble(
//                            itemIdSource.getItemIds()
//                                    .retryWhen(new RetryWithDelay(scheduleConfiguration.getRetryJobDuration()))
//                    )
//                            /**
//                             * This operator plays critical role in "reactive pull" implementation, because it's blocking
//                             * assembler thread pool when {@link ItemsSender} consumes assembled items slower than
//                             * assembler is producing them. Such situation can occur when one {@link ItemsSender} is
//                             * significantly slower than others. In this case
//                             * {@link rx.observables.ConnectableObservable} in {@link ItemsSender} will block assembler
//                             * threads, so they won't produce more items.
//                             */
//                            .onBackpressureBlock()
//            )
//                    .doOnCompleted(new Action0() {
//                        @Override
//                        public void call() {
//                            log.info("All of senders are completed.");
//                        }
//                    })
//                    .toBlocking()
//                    .forEach(new Action1<Pair<String, SendResult>>() {
//                        @Override
//                        public void call(Pair<String, SendResult> pair) {
//                            eventBus.post(new ItemSenderCompletedEvent(pair.getLeft()));
//                            sendersDeadItemUuidQueue.addFailed(pair);
//                            log.info("{} is completed ({} request[s]) ({} item[s])",
//                                    pair.getLeft(), pair.getRight().getMergeCount(),
//                                    pair.getRight().getSuccessCount());
//                        }
//                    });
//        }
//        finally {
//            log.info("IndexingCompletedEvent sent");
//            eventBus.post(IndexingCompletedEvent.getInstance());
//            cycleNumber.addAndGet(1);
//            log.info("Items indexing completed");
//        }
//    }
//
//    /**
//     * Function for {@link Observable#retryWhen(Func1)} operator which re-subscribed to a given stream if there was
//     * {@link rx.Observer#onError(Throwable)} with {@link ItemIdSourceException}. Retry duration can be specified in
//     * {@link com.groupon.relevance.darwin.indexer.Indexer.RetryWithDelay()} constructor.
//     */
//    private static class RetryWithDelay implements Func1<Observable<? extends Throwable>, Observable<?>> {
//
//        private final static Logger log = LoggerFactory.getLogger(RetryWithDelay.class);
//
//        private final Duration retryDuration;
//
//        /**
//         * @param retryDuration retry duration
//         */
//        private RetryWithDelay(Duration retryDuration) {
//            this.retryDuration = retryDuration;
//        }
//
//        @Override
//        public Observable<?> call(Observable<? extends Throwable> attempts) {
//            return attempts.flatMap(new Func1<Throwable, Observable<?>>() {
//                @Override
//                public Observable<?> call(Throwable throwable) {
//                    if (throwable instanceof ItemIdSourceException ||
//                        throwable instanceof RuntimeItemIdSourceException) {
//                        log.error("Job could not run due to {}. Apart from standard schedule, this job will be " +
//                                  "re-triggered in {}", throwable.getClass().getSimpleName(), retryDuration, throwable);
//                        // When this Observable calls onNext, the original
//                        // Observable will be retried (i.e. re-subscribed).
//                        return Observable.timer(retryDuration.toMilliseconds(), TimeUnit.MILLISECONDS);
//                    } else {
//                        log.error("Job could not run due to RuntimeException. This job won't be re-triggered",
//                                  throwable);
//                        // Not ItemIdSourceException or RuntimeItemIdSourceException. Just pass the error along.
//                        return Observable.error(throwable);
//                    }
//                }
//            });
//        }
//    }
//
//}
