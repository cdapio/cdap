package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.Split;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.util.OperationUtil;
import com.google.common.base.Charsets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class StreamTest {

  private static OmidTransactionalOperationExecutor executor;
  private static OperationContext context = OperationUtil.DEFAULT;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
    executor = (OmidTransactionalOperationExecutor)injector.getInstance(OperationExecutor.class);
  }


  @Test
  public void testStream() throws OperationException, InterruptedException {
    Stream stream = new Stream("testStream",executor.getTableHandle());
    OmidTransactionalOperationExecutor.StreamMetaOracle.setOffsetWriteIntervalSeconds(1);
    long startTime = System.currentTimeMillis()/1000;

    long endTime = startTime  + 60*60; // 1hr from startTime

    stream.setStartTime(startTime);
    stream.setEndTime(endTime);

    byte [] streamKeyPrefix = "stream://testStream".getBytes(Charsets.UTF_8);

    List<Split> splits = stream.getSplits();
    assertTrue(0 == splits.size());

    //insert n times into streams
    int count = writeNTimes(10, streamKeyPrefix);
    assertEquals(10, count);

    Thread.sleep(1000);

    //insert n times into streams
    count = writeNTimes(10, streamKeyPrefix);
    assertEquals(10, count);


    splits = stream.getSplits();
    assertEquals(1, splits.size());



  }

  private int writeNTimes(int numTimes, byte [] keyPrefix) throws OperationException {
    int count = 0;
    for (int i=0; i<numTimes;i++){
      byte [] val = Bytes.toBytes(count);
      count++;
      executor.commit(context, new QueueEnqueue(keyPrefix, new QueueEntry(val)));
    }
    return count;
  }

}
