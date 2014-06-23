package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.test.XSlowTests;
import org.junit.experimental.categories.Category;

/**
 * Queue test implementation running on HBase 0.96.
 */
@Category(XSlowTests.class)
public class HBase96QueueTest extends HBaseQueueTest {
  // nothing to override
}
