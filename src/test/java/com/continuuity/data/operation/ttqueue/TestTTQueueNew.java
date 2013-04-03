package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.DequeueEntry;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.DequeuedEntrySet;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.WorkingEntryList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestTTQueueNew {

  // Test DequeueEntry
  @Test
  public void testDequeueEntryEncode() throws Exception {
    DequeueEntry expectedEntry = new DequeueEntry(1, 2);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntry.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    DequeueEntry actualEntry = DequeueEntry.decode(new BinaryDecoder(new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
    assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    assertEquals(expectedEntry, actualEntry);
  }

  @Test
  public void testDequeueEntryEquals() throws Exception {
    // DequeueEntry is only compared using entryId, tries is ignored
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12));
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12, 10));
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12, 15));

    assertNotEquals(new DequeueEntry(12, 10), new DequeueEntry(13, 10));
    assertNotEquals(new DequeueEntry(12, 10), new DequeueEntry(13, 11));
  }

  @Test
  public void testDequeueEntryCompare() throws Exception {
    // DequeueEntry is compared only on entryId, tries is ignored
    SortedSet<DequeueEntry> sortedSet = new TreeSet<DequeueEntry>();
    sortedSet.add(new DequeueEntry(5, 1));
    sortedSet.add(new DequeueEntry(2, 3));
    sortedSet.add(new DequeueEntry(1, 2));
    sortedSet.add(new DequeueEntry(3, 2));
    sortedSet.add(new DequeueEntry(4, 3));
    sortedSet.add(new DequeueEntry(4, 5));
    sortedSet.add(new DequeueEntry(0, 2));

    int expected = 0;
    for(Iterator<DequeueEntry> iterator = sortedSet.iterator(); iterator.hasNext(); ) {
      assertEquals(expected++, iterator.next().getEntryId());
    }
  }

  @Test
  public void testQueueEntrySetEncode() throws Exception {
    final int MAX = 10;
    DequeuedEntrySet expectedEntrySet = new DequeuedEntrySet();
    for(int i = 0; i < MAX; ++i) {
      expectedEntrySet.add(new DequeueEntry(i, i % 2));
    }

    assertEquals(MAX, expectedEntrySet.size());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntrySet.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    DequeuedEntrySet actualEntrySet = DequeuedEntrySet.decode(new BinaryDecoder(new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntrySet.size(), actualEntrySet.size());
    for(int i = 0; i < MAX; ++i) {
      DequeueEntry expectedEntry = expectedEntrySet.min();
      expectedEntrySet.remove(expectedEntry.getEntryId());

      DequeueEntry actualEntry = actualEntrySet.min();
      actualEntrySet.remove(actualEntry.getEntryId());

      assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
      assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    }
  }

  @Test
  public void testQueueEntrySet() throws Exception {
    final int MAX = 10;
    DequeuedEntrySet entrySet = new DequeuedEntrySet();
    List<Long> expectedEntryIds = Lists.newArrayListWithCapacity(MAX);
    List<DequeueEntry> expectedEntryList = Lists.newArrayListWithCapacity(MAX);
    Set<Long> expectedDroppedEntries = Sets.newHashSetWithExpectedSize(MAX);

    for(int i = 0; i < MAX; ++i) {
      entrySet.add(new DequeueEntry(i, i %2));
      expectedEntryIds.add((long) i);
      expectedEntryList.add(new DequeueEntry(i, i % 2));

      if(i % 2 == 1) {
        expectedDroppedEntries.add((long) i);
      }
    }

    // Verify created lists
    assertEquals(MAX, entrySet.size());
    assertEquals(MAX, expectedEntryIds.size());
    assertEquals(MAX, expectedEntryList.size());
    assertEquals(MAX/2, expectedDroppedEntries.size());

    // Verify QueueEntrySet
    assertEquals(expectedEntryIds, entrySet.getEntryIds());
    assertEquals(expectedEntryList, entrySet.getEntryList());

    Set<Long> actualDroppedEntries = entrySet.startNewTry(1);
    assertEquals(expectedDroppedEntries, actualDroppedEntries);

    List<Long> actualRemainingEntries = Lists.newArrayListWithCapacity(MAX);
    for(int i = 0; i < MAX; ++i) {
      if(i % 2 == 0) {
        actualRemainingEntries.add((long) i);
      }
    }
    assertEquals(actualRemainingEntries, entrySet.getEntryIds());
  }

  @Test
  public void testWorkingEntryList() {
    final int MAX = 10;
    WorkingEntryList workingEntryList = new WorkingEntryList(Lists.<DequeueEntry>newArrayList());
    assertFalse(workingEntryList.hasNext());

    for(int i = 0; i < MAX; ++i) {
      workingEntryList.add(new DequeueEntry(i));
    }

    for(int i = 0; i < MAX; ++i) {
      assertTrue(workingEntryList.hasNext());
      assertEquals(new DequeueEntry(i), workingEntryList.peekNext());
      assertEquals(new DequeueEntry(i), workingEntryList.next());
    }
    assertFalse(workingEntryList.hasNext());

    for(int i = 0; i < MAX; ++i) {
      workingEntryList.add(new DequeueEntry(i));
    }

    for(int i = 0; i < MAX; ++i) {
      assertTrue(workingEntryList.hasNext());
      assertEquals(new DequeueEntry(i), workingEntryList.peekNext());
      assertEquals(new DequeueEntry(i), workingEntryList.next());
    }
    assertFalse(workingEntryList.hasNext());
  }
}
