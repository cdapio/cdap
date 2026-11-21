/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.messaging.service;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.tephra.TxConstants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Direct unit tests for {@link CoreMessagingService} exception propagation and resource cleanup.
 */
public class CoreMessagingServiceTest {

  @Test
  public void testPublishAndStorePayloadPropagateTopicNotFoundAndIoException() throws Exception {
    TopicId missingTopic = NamespaceId.DEFAULT.topic("missing");
    TopicId ioTopic = NamespaceId.DEFAULT.topic("ioTopic");
    TopicId payloadIoTopic = NamespaceId.DEFAULT.topic("payloadIoTopic");

    final TopicNotFoundException expectedNotFound =
        new TopicNotFoundException(missingTopic.getNamespace(), missingTopic.getTopic());
    final IOException expectedMessageIo = new IOException("createMessageTable failed");
    final IOException expectedPayloadIo = new IOException("createPayloadTable failed");

    MetadataTable metadataTable = Mockito.mock(MetadataTable.class);
    Mockito.when(metadataTable.getMetadata(missingTopic)).thenThrow(expectedNotFound);
    Mockito.when(metadataTable.getMetadata(ioTopic))
        .thenReturn(new DefaultTopicMetadata(ioTopic, "ttl", "3600", "generation", "1"));
    Mockito.when(metadataTable.getMetadata(payloadIoTopic))
        .thenReturn(new DefaultTopicMetadata(payloadIoTopic, "ttl", "3600", "generation", "1"));

    TableFactory tableFactory = Mockito.mock(TableFactory.class);
    Mockito.when(tableFactory.createMetadataTable()).thenReturn(metadataTable);
    Mockito.when(tableFactory.createMessageTable(Mockito.any())).thenThrow(expectedMessageIo);
    Mockito.when(tableFactory.createPayloadTable(Mockito.any())).thenThrow(expectedPayloadIo);

    CoreMessagingService service = createService(tableFactory);

    StoreRequest missingRequest = StoreRequestBuilder.of(missingTopic).addPayload("a").build();
    try {
      service.publish(missingRequest);
      Assert.fail("Expected TopicNotFoundException from publish");
    } catch (TopicNotFoundException actual) {
      Assert.assertSame(expectedNotFound, actual);
    }

    try {
      service.storePayload(
          StoreRequestBuilder.of(missingTopic).addPayload("a").setTransaction(1L).build());
      Assert.fail("Expected TopicNotFoundException from storePayload");
    } catch (TopicNotFoundException actual) {
      Assert.assertSame(expectedNotFound, actual);
    }

    try {
      service.publish(StoreRequestBuilder.of(ioTopic).addPayload("a").build());
      Assert.fail("Expected IOException from publish");
    } catch (IOException actual) {
      Assert.assertSame(expectedMessageIo, actual);
    }

    try {
      service.storePayload(
          StoreRequestBuilder.of(payloadIoTopic).addPayload("a").setTransaction(1L).build());
      Assert.fail("Expected IOException from storePayload");
    } catch (IOException actual) {
      Assert.assertSame(expectedPayloadIo, actual);
    }
  }

  @Test
  public void testGetTopicUnwrapsCheckedCauseDirectlyWhileWriterCacheWrapsExecutionException()
      throws Exception {
    TopicId topicGetChecked = NamespaceId.DEFAULT.topic("topicGetChecked");
    TopicId topicPublishChecked = NamespaceId.DEFAULT.topic("topicPublishChecked");
    TopicId topicPayloadChecked = NamespaceId.DEFAULT.topic("topicPayloadChecked");
    TopicId topicRuntime = NamespaceId.DEFAULT.topic("topicRuntime");

    final Exception expectedTopicChecked = new Exception("checked metadata failure");
    final Exception expectedMessageChecked = new Exception("checked message table failure");
    final Exception expectedPayloadChecked = new Exception("checked payload table failure");
    final IllegalStateException expectedRuntime = new IllegalStateException("unchecked failure");

    MetadataTable metadataTable = Mockito.mock(MetadataTable.class);
    Mockito.when(metadataTable.getMetadata(topicGetChecked)).thenAnswer(inv -> {
      throw expectedTopicChecked;
    });
    Mockito.when(metadataTable.getMetadata(topicPublishChecked))
        .thenReturn(new DefaultTopicMetadata(topicPublishChecked, "ttl", "3600", "generation", "1"));
    Mockito.when(metadataTable.getMetadata(topicPayloadChecked))
        .thenReturn(new DefaultTopicMetadata(topicPayloadChecked, "ttl", "3600", "generation", "1"));
    Mockito.when(metadataTable.getMetadata(topicRuntime))
        .thenReturn(new DefaultTopicMetadata(topicRuntime, "ttl", "3600", "generation", "1"));

    TableFactory tableFactory = Mockito.mock(TableFactory.class);
    Mockito.when(tableFactory.createMetadataTable()).thenReturn(metadataTable);
    Mockito.when(tableFactory.createMessageTable(Mockito.argThat(
        meta -> meta != null && topicPublishChecked.equals(meta.getTopicId()))))
        .thenAnswer(inv -> {
          throw expectedMessageChecked;
        });
    Mockito.when(tableFactory.createPayloadTable(Mockito.argThat(
        meta -> meta != null && topicPayloadChecked.equals(meta.getTopicId()))))
        .thenAnswer(inv -> {
          throw expectedPayloadChecked;
        });
    Mockito.when(tableFactory.createPayloadTable(Mockito.argThat(
        meta -> meta != null && topicRuntime.equals(meta.getTopicId()))))
        .thenThrow(expectedRuntime);

    CoreMessagingService service = createService(tableFactory);

    // getTopic() unwraps ExecutionException so RuntimeException.getCause() is the checked cause C
    try {
      service.getTopicMetadataProperties(topicGetChecked);
      Assert.fail("Expected RuntimeException wrapping checked cause directly");
    } catch (RuntimeException actual) {
      Assert.assertFalse(actual instanceof UncheckedExecutionException);
      Assert.assertSame(expectedTopicChecked, actual.getCause());
    }

    // publish() catches ExecutionException and wraps via Throwables.propagate(e)
    try {
      service.publish(StoreRequestBuilder.of(topicPublishChecked).addPayload("a").build());
      Assert.fail("Expected RuntimeException from publish");
    } catch (RuntimeException actual) {
      Assert.assertTrue(actual.getCause() instanceof ExecutionException);
      Assert.assertSame(expectedMessageChecked, actual.getCause().getCause());
    }

    // storePayload() catches ExecutionException and wraps via Throwables.propagate(e)
    try {
      service.storePayload(
          StoreRequestBuilder.of(topicPayloadChecked).addPayload("a").setTransaction(1L).build());
      Assert.fail("Expected RuntimeException from storePayload");
    } catch (RuntimeException actual) {
      Assert.assertTrue(actual.getCause() instanceof ExecutionException);
      Assert.assertSame(expectedPayloadChecked, actual.getCause().getCause());
    }

    // Unchecked RuntimeException from CacheLoader surfaces as UncheckedExecutionException
    try {
      service.storePayload(
          StoreRequestBuilder.of(topicRuntime).addPayload("a").setTransaction(1L).build());
      Assert.fail("Expected UncheckedExecutionException");
    } catch (UncheckedExecutionException actual) {
      Assert.assertSame(expectedRuntime, actual.getCause());
    }
  }

  @Test
  public void testRollbackPropagatesUncheckedAndCheckedExceptionsCorrectly() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("rollbackTopic");
    MetadataTable metadataTable = Mockito.mock(MetadataTable.class);
    Mockito.when(metadataTable.getMetadata(topicId))
        .thenReturn(new DefaultTopicMetadata(topicId, "ttl", "3600", "generation", "1"));

    final IllegalStateException expectedRuntime = new IllegalStateException("rollback runtime");
    MessageTable runtimeTable = Mockito.mock(MessageTable.class);
    Mockito.doThrow(expectedRuntime)
        .when(runtimeTable).rollback(Mockito.any(), Mockito.any());

    final IOException expectedIo = new IOException("rollback io");
    MessageTable ioTable = Mockito.mock(MessageTable.class);
    Mockito.doThrow(expectedIo)
        .when(ioTable).rollback(Mockito.any(), Mockito.any());

    final Exception expectedChecked = new Exception("rollback checked");
    MessageTable checkedTable = Mockito.mock(MessageTable.class);
    Mockito.doAnswer(inv -> {
      throw expectedChecked;
    }).when(checkedTable).rollback(Mockito.any(), Mockito.any());

    TableFactory tableFactory = Mockito.mock(TableFactory.class);
    Mockito.when(tableFactory.createMetadataTable()).thenReturn(metadataTable);
    Mockito.when(tableFactory.createMessageTable(Mockito.any()))
        .thenReturn(runtimeTable, ioTable, checkedTable);

    CoreMessagingService service = createService(tableFactory);
    RollbackDetail rollbackDetail = Mockito.mock(RollbackDetail.class);

    // Unchecked RuntimeException must NOT be double-wrapped
    try {
      service.rollback(topicId, rollbackDetail);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    // IOException propagates directly
    try {
      service.rollback(topicId, rollbackDetail);
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(expectedIo, actual);
    }

    // Other checked Exception is wrapped in RuntimeException with cause == expectedChecked
    try {
      service.rollback(topicId, rollbackDetail);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedChecked, actual.getCause());
    }
  }

  @Test
  public void testFetchPayloadReferencePropagatesCreatePayloadTableIoException() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("fetchPayloadFail");
    MetadataTable metadataTable = Mockito.mock(MetadataTable.class);
    Mockito.when(metadataTable.getMetadata(topicId))
        .thenReturn(new DefaultTopicMetadata(topicId, "ttl", "3600", "generation", "1"));

    MessageTable.Entry payloadRefEntry = Mockito.mock(MessageTable.Entry.class);
    Mockito.when(payloadRefEntry.isPayloadReference()).thenReturn(true);
    Mockito.when(payloadRefEntry.getPublishTimestamp()).thenReturn(System.currentTimeMillis());
    Mockito.when(payloadRefEntry.getSequenceId()).thenReturn((short) 0);
    Mockito.when(payloadRefEntry.getTransactionWritePointer()).thenReturn(10L);

    MessageTable messageTable = Mockito.mock(MessageTable.class);
    Mockito.when(messageTable.fetch(Mockito.any(), Mockito.anyLong(), Mockito.anyInt(), Mockito.any()))
        .thenReturn(new AbstractCloseableIterator<MessageTable.Entry>() {
          private boolean consumed;

          @Override
          protected MessageTable.Entry computeNext() {
            if (!consumed) {
              consumed = true;
              return payloadRefEntry;
            }
            return endOfData();
          }

          @Override
          public void close() {
            // no-op
          }
        });

    final IOException expectedPayloadIo = new IOException("payload table open failure");
    TableFactory tableFactory = Mockito.mock(TableFactory.class);
    Mockito.when(tableFactory.createMetadataTable()).thenReturn(metadataTable);
    Mockito.when(tableFactory.createMessageTable(Mockito.any())).thenReturn(messageTable);
    Mockito.when(tableFactory.createPayloadTable(Mockito.any())).thenThrow(expectedPayloadIo);

    CoreMessagingService service = createService(tableFactory);
    try (CloseableIterator<?> iterator = service.fetch(
        new DefaultMessageFetchRequest.Builder().setTopicId(topicId).build())) {
      try {
        iterator.hasNext();
        Assert.fail("Expected RuntimeException wrapping payload IOException");
      } catch (RuntimeException actual) {
        Assert.assertSame(expectedPayloadIo, actual.getCause());
      }
    }
  }

  @Test
  public void testShutDownClosesTableFactoryQuietlyOnIoException() throws Exception {
    final AtomicBoolean closeCalled = new AtomicBoolean(false);
    MetadataTable metadataTable = Mockito.mock(MetadataTable.class);
    TableFactory tableFactory = Mockito.mock(TableFactory.class);
    Mockito.when(tableFactory.createMetadataTable()).thenReturn(metadataTable);
    Mockito.doAnswer(inv -> {
      closeCalled.set(true);
      throw new IOException("tableFactory close failed");
    }).when(tableFactory).close();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "");
    CoreMessagingService service = new CoreMessagingService(
        cConf, tableFactory, TimeProvider.SYSTEM_TIME, new NoOpMetricsCollectionService());
    service.startAsync().awaitRunning();
    service.stopAsync().awaitTerminated();
    Assert.assertTrue(closeCalled.get());
  }

  private static CoreMessagingService createService(TableFactory tableFactory) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10000000000L);
    return new CoreMessagingService(
        cConf, tableFactory, TimeProvider.SYSTEM_TIME, new NoOpMetricsCollectionService());
  }
}
