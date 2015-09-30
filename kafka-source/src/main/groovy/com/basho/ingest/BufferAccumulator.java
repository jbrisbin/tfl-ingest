package com.basho.ingest;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.reactivestreams.PublisherFactory;
import reactor.core.reactivestreams.SubscriberBarrier;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;

/**
 * Created by jbrisbin on 9/28/15.
 */
public class BufferAccumulator implements Function<Publisher<Buffer>, Publisher<Buffer>> {

    @Override
    public Publisher<Buffer> apply(Publisher<Buffer> in) {
        return PublisherFactory.<Buffer,Buffer>intercept(in, AccumulatorBarrier::new);
    }

    private class AccumulatorBarrier extends SubscriberBarrier<Buffer, Buffer> {
        private Buffer buf = new Buffer();

        public AccumulatorBarrier(Subscriber<? super Buffer> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doNext(Buffer bytes) {
            buf.append(bytes);
        }

        @Override
        protected void doComplete() {
            subscriber.onNext(buf.flip());
            subscriber.onComplete();
        }
    }

}
