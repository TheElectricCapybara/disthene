package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.Disthene;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import net.iponweb.disthene.util.NameThreadFactory;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrei Ivanov
 */
public class BatchMetricProcessor {
    private static final String SCHEDULER_NAME = "distheneCassandraProcessor";


    private static final String QUERY = "UPDATE metric.metric USING TTL ? SET data = data + ? WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;";

    private Logger logger = Logger.getLogger(BatchMetricProcessor.class);

    private Session session;
    private PreparedStatement statement;
    private int batchSize;
    private Queue<Metric> metrics = new LinkedBlockingQueue<>();
    private AtomicBoolean executing = new AtomicBoolean(false);
    private MBassador<DistheneEvent> bus;
    private RateLimiter rateLimiter;

    private final ThreadPoolExecutor tpExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    private final Executor executor = MoreExecutors.listeningDecorator(tpExecutor);
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NameThreadFactory(SCHEDULER_NAME));
    private ScheduledExecutorService statsScheduler = Executors.newScheduledThreadPool(1, new NameThreadFactory(SCHEDULER_NAME));


    public BatchMetricProcessor(final Session session, final int batchSize, int flushInterval, int maxThroughput, final MBassador<DistheneEvent> bus) {
        this.session = session;
        this.batchSize = batchSize;
        this.bus = bus;

        statement = session.prepare(QUERY);

        rateLimiter = RateLimiter.create(maxThroughput / (60 * batchSize));

        //todo: remove debug
        statsScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.debug("****Threads in the pool: " + tpExecutor.getPoolSize());
                logger.debug("****Bus has pending messages: " + Disthene.dispatch.getPendingMessages().size());
                logger.debug("****Metrics in queue: " + metrics.size());
            }
        }, 0, 30, TimeUnit.SECONDS);

/*
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
*/

        ThreadFactory flusherFactory =  new NameThreadFactory("distheneCassandraWriters");
        for (int i = 0; i < 10; i++) {
            Thread flusher = flusherFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    int currentBatchSize = 0;
                    BatchStatement batch = new BatchStatement();

                    while (true) {
                        Metric metric = metrics.poll();

                        if (metric != null) {
                            batch.add(
                                    statement.bind(
                                            metric.getRollup() * metric.getPeriod(),
                                            Collections.singletonList(metric.getValue()),
                                            metric.getTenant(),
                                            metric.getRollup(),
                                            metric.getPeriod(),
                                            metric.getPath(),
                                            metric.getUnixTimestamp()
                                    )
                            );
                            currentBatchSize++;

                            if (currentBatchSize == batchSize) {
                                ResultSetFuture future = session.executeAsync(batch);

                                Futures.addCallback(future,
                                        new FutureCallback<ResultSet>() {
                                            @Override
                                            public void onSuccess(ResultSet result) {
                                            }

                                            @Override
                                            public void onFailure(Throwable t) {
                                                logger.error(t);
                                            }
                                        },
                                        executor
                                );

                                currentBatchSize = 0;
                                batch = new BatchStatement();
                            }
                        }
                    }
                }
            });

            flusher.start();
        }
    }


    public void add(Metric metric) {
        metrics.offer(metric);
//        executeIfNeeded();
    }

    private void executeIfNeeded() {
        if (batchSize != -1 && metrics.size() >= batchSize) {
            if (executing.compareAndSet(false, true)) {
                execute(batchSize);
            }
        }
    }

    private void flush() {
        if (metrics.size() > 0) {
                execute(batchSize);
        }
    }

    private synchronized void execute(int minBatchSize) {

        while (metrics.size() >= minBatchSize) {
            int currentBatchSize = 0;
            final BatchStatement batch = new BatchStatement();

            while (currentBatchSize < batchSize && metrics.size() > 0) {
                Metric metric = metrics.poll();
                batch.add(
                    statement.bind(
                            metric.getRollup() * metric.getPeriod(),
                            Collections.singletonList(metric.getValue()),
                            metric.getTenant(),
                            metric.getRollup(),
                            metric.getPeriod(),
                            metric.getPath(),
                            metric.getUnixTimestamp()
                    )
                );
                currentBatchSize++;
            }

            // todo: probably change to warning or remove
            double wt = rateLimiter.acquire(1);
            if (wt > 0.1) {
                logger.debug("C* was throttled for " + wt);
            }
            ResultSetFuture future = session.executeAsync(batch);

            final int finalCurrentBatchSize = currentBatchSize;
            Futures.addCallback(future,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet result) {
//                            bus.post(new StoreSuccessEvent(finalCurrentBatchSize)).asynchronously();
                        }

                        @Override
                        public void onFailure(Throwable t) {
//                            bus.post(new StoreErrorEvent(finalCurrentBatchSize)).asynchronously();
                            logger.error(t);
                            // let's also penalize an error by additionally throttling C* writes
                            // effectively let's suspend it for like 10 seconds hoping C* will recover after that
                            rateLimiter.acquire((int) (10 * rateLimiter.getRate()));
                        }
                    },
                    executor
            );
        }

        executing.set(false);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

}
