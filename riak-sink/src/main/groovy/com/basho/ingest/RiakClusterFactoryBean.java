package com.basho.ingest;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.Lifecycle;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jbrisbin on 9/28/15.
 */
@Component
public class RiakClusterFactoryBean implements FactoryBean<RiakCluster>, Lifecycle {

    private final RiakCluster cluster;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public RiakClusterFactoryBean() throws UnknownHostException {
        this.cluster = RiakCluster.builder(new RiakNode.Builder().build()).build();
    }

    @Override
    public RiakCluster getObject() throws Exception {
        return cluster;
    }

    @Override
    public Class<?> getObjectType() {
        return RiakCluster.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            this.cluster.start();
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            this.cluster.shutdown();
        }
    }

    @Override
    public boolean isRunning() {
        return started.get();
    }

}
