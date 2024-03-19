package net.iponweb.disthene.carbon;

import com.google.common.base.CharMatcher;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricReceivedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LogManager.getLogger(CarbonServerHandler.class);

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;

    public CarbonServerHandler(MBassador<DistheneEvent> bus, Rollup rollup) {
        this.bus = bus;
        this.rollup = rollup;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;

        try {
            final Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), rollup);
            if ((System.currentTimeMillis() / 1000L) - metric.getTimestamp() > 3600) {
                logger.warn("Metric is from distant past (older than 1 hour): " + metric);
            }

            if (CharMatcher.ascii().matchesAllOf(metric.getPath()) && CharMatcher.ascii().matchesAllOf(metric.getTenant())) {
                bus.post(new MetricReceivedEvent(metric)).now();
            } else {
                logger.warn("Non ASCII characters received, discarding: " + metric);
            }
        } catch (Exception e) {
            logger.trace(e);
        }

        in.release();
    }
}
