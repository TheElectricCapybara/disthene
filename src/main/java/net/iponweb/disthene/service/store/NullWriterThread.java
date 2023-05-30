package net.iponweb.disthene.service.store;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;

import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
public class NullWriterThread extends WriterThread {

    public NullWriterThread(String name, MBassador<DistheneEvent> bus, Session session, PreparedStatement statement, Queue<Metric> metrics, Executor executor) {
        super(name, bus, session, statement, metrics, executor);
    }

    @Override
    public void run() {
        while (!shutdown) {
            Metric metric = metrics.poll();
            if (metric != null) {
                store(metric);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void store(Metric metric) {
        bus.post(new StoreSuccessEvent(1)).now();
    }

    @Override
    public void shutdown() {
    }
}