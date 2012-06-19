package com.continuuity.common.conflake;

import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

/**
 *
 */
public class UniqueIDGeneratorTest {

  public class IDGenerator implements Runnable {
    private final UniqueIDGenerator idGenerator;
    private final List<Long> clash = new ArrayList<Long>();

    public IDGenerator(long datacenterId, long workerId) {
      idGenerator = new UniqueIDGenerator(datacenterId, workerId);
    }

    @Override
    public void run() {
      for(int i = 0; i < 10000; ++i) {
        long id = idGenerator.next();
        clash.add(id);
      }
    }

    Collection<Long> getClashMap() {
      return clash;
    }
  }

  @Test
  public void testSimple() throws Exception {
    IDGenerator id1 = new IDGenerator(1, 1);
    IDGenerator id2 = new IDGenerator(1, 2);

    Thread thread1 = new Thread(id1);
    Thread thread2 = new Thread(id2);

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    Collection<Long> gen1 = id1.getClashMap();
    Collection<Long> gen2 = id2.getClashMap();

    gen1.retainAll(gen2);
    Assert.assertTrue(gen1.size() == 0);
  }
}
