package com.basho.ingest;

import com.basho.riak.client.api.RiakClient;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.Lifecycle;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jbrisbin on 9/28/15.
 */
@Component
public class RiakClientFactoryBean implements FactoryBean<RiakClient>, Lifecycle {

    private final RiakClient client;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public RiakClientFactoryBean() throws UnknownHostException {
        this.client = RiakClient.newClient();
    }

    @Override
    public RiakClient getObject() throws Exception {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return RiakClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void start() {
        started.compareAndSet(false, true);
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            this.client.shutdown();
        }
    }

    @Override
    public boolean isRunning() {
        return started.get();
    }

}
