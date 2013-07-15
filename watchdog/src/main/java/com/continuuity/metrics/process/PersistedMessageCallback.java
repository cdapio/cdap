package com.continuuity.metrics.process;

import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaConsumer;

import java.util.Iterator;

/**
 * A {@link KafkaConsumer.MessageCallback} that persists offset information into a VCTable while
 * delegating the actual message consumption to another {@link KafkaConsumer.MessageCallback}.
 */
public final class PersistedMessageCallback implements KafkaConsumer.MessageCallback {

  private final KafkaConsumer.MessageCallback delegate;
  private final VersionedColumnarTable metaTable;

  public PersistedMessageCallback(KafkaConsumer.MessageCallback delegate, VersionedColumnarTable metaTable) {
    this.delegate = delegate;
    this.metaTable = metaTable;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {
//    try {
//      OffsetTrackingIterator itor = new OffsetTrackingIterator(messages);
//      delegate.onReceived(itor);
//
//      // Make sure the iterator is exhausted
//      while (itor.hasNext()) {
//        itor.next();
//      }
//
//    } catch (Throwable t) {
//      throw Throwables.propagate(t);
//    }
  }

  @Override
  public void finished(boolean error, Throwable cause) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
