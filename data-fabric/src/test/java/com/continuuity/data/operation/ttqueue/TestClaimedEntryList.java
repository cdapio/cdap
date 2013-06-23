package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestClaimedEntryList {

  @Test
  public void testClaimedEntry() {
    ClaimedEntryRange claimedEntryRange = ClaimedEntryRange.INVALID;
    for(int i = 0; i < 10; ++i) {
      assertFalse(claimedEntryRange.isValid());
      claimedEntryRange.move(1);
    }
    assertFalse(claimedEntryRange.isValid());

    claimedEntryRange = new ClaimedEntryRange(0, 5);
    for(int i = 0; i < 6; ++i) {
      assertTrue(claimedEntryRange.isValid());
      claimedEntryRange.move(claimedEntryRange.getBegin() + 1);
    }
    assertFalse(claimedEntryRange.isValid());

    try {
      new ClaimedEntryRange(4, 2);
      fail("Should throw exception");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      new ClaimedEntryRange(TTQueueNewOnVCTable.INVALID_ENTRY_ID, 2);
      fail("Should throw exception");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      new ClaimedEntryRange(3, TTQueueNewOnVCTable.INVALID_ENTRY_ID);
      fail("Should throw exception");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testClaimedEntryListMove() {
    ClaimedEntryList claimedEntryList = new ClaimedEntryList();
    for(int i = 0; i < 10; ++i) {
      assertFalse(claimedEntryList.getClaimedEntry().isValid());
      claimedEntryList.moveForwardTo(1);
    }
    assertFalse(claimedEntryList.getClaimedEntry().isValid());

    claimedEntryList.add(1, 5);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 1, 5);
    assertFalse(claimedEntryList.getClaimedEntry().isValid());

    claimedEntryList.add(2, 9);
    claimedEntryList.add(11, 20);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 2, 9);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 11, 20);
    assertFalse(claimedEntryList.getClaimedEntry().isValid());

    claimedEntryList.add(TTQueueNewOnVCTable.INVALID_ENTRY_ID, TTQueueNewOnVCTable.INVALID_ENTRY_ID);
    claimedEntryList.add(6, 6);
    claimedEntryList.add(8, 30);
    claimedEntryList.add(TTQueueNewOnVCTable.INVALID_ENTRY_ID, TTQueueNewOnVCTable.INVALID_ENTRY_ID);
    claimedEntryList.add(3, 4);
    claimedEntryList.add(1, 2);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 1, 2);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 3, 4);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 6, 6);
    verifyClaimedEntryListIncrementMove(claimedEntryList, 8, 30);
    assertFalse(claimedEntryList.getClaimedEntry().isValid());

    claimedEntryList.add(5, 10);
    claimedEntryList.add(1, 3);
    assertTrue(claimedEntryList.getClaimedEntry().isValid());
    assertEquals(1, claimedEntryList.getClaimedEntry().getBegin());
    assertEquals(3, claimedEntryList.getClaimedEntry().getEnd());

    claimedEntryList.moveForwardTo(5);
    assertTrue(claimedEntryList.getClaimedEntry().isValid());
    assertEquals(5, claimedEntryList.getClaimedEntry().getBegin());
    assertEquals(10, claimedEntryList.getClaimedEntry().getEnd());

    claimedEntryList.moveForwardTo(8);
    assertTrue(claimedEntryList.getClaimedEntry().isValid());
    assertEquals(8, claimedEntryList.getClaimedEntry().getBegin());
    assertEquals(10, claimedEntryList.getClaimedEntry().getEnd());

    try {
      claimedEntryList.moveForwardTo(0);
      fail("Exception should be thrown here");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    claimedEntryList.moveForwardTo(15);
    assertFalse(claimedEntryList.getClaimedEntry().isValid());
  }

  @Test
  public void testClaimedEntryListAddAll() throws Exception {
    ClaimedEntryList claimedEntryList1 = new ClaimedEntryList();
    claimedEntryList1.add(3, 5);
    claimedEntryList1.add(15, 20);
    claimedEntryList1.add(36, 41);

    ClaimedEntryList claimedEntryList2 = new ClaimedEntryList();
    claimedEntryList2.add(1, 2);
    claimedEntryList2.add(10, 14);
    claimedEntryList2.add(45, 50);

    claimedEntryList1.addAll(claimedEntryList2);

    verifyClaimedEntryListIncrementMove(claimedEntryList1, 1, 2);
    verifyClaimedEntryListIncrementMove(claimedEntryList1, 3, 5);
    verifyClaimedEntryListIncrementMove(claimedEntryList1, 10, 14);
    verifyClaimedEntryListIncrementMove(claimedEntryList1, 15, 20);
    verifyClaimedEntryListIncrementMove(claimedEntryList1, 36, 41);
    verifyClaimedEntryListIncrementMove(claimedEntryList1, 45, 50);
    assertFalse(claimedEntryList1.getClaimedEntry().isValid());
  }

  private void verifyClaimedEntryListIncrementMove(ClaimedEntryList claimedEntryList, long begin, long end) {
    assertTrue(claimedEntryList.getClaimedEntry().isValid());
    assertEquals(begin, claimedEntryList.getClaimedEntry().getBegin());
    assertEquals(end, claimedEntryList.getClaimedEntry().getEnd());

    for(long i = begin; i <= end; ++i, claimedEntryList.moveForwardTo(claimedEntryList.getClaimedEntry().getBegin() + 1)) {
      assertTrue(claimedEntryList.getClaimedEntry().isValid());
      assertEquals(i, claimedEntryList.getClaimedEntry().getBegin());
      assertEquals(end, claimedEntryList.getClaimedEntry().getEnd());
    }
  }

  @Test
  public void testClaimedEntryListEncode() throws Exception {
    ClaimedEntryList claimedEntryList = new ClaimedEntryList();
    verifyClaimedEntryListEncode(claimedEntryList);

    claimedEntryList.add(5, 5);
    verifyClaimedEntryListEncode(claimedEntryList);

    claimedEntryList.add(TTQueueNewOnVCTable.INVALID_ENTRY_ID, TTQueueNewOnVCTable.INVALID_ENTRY_ID);
    claimedEntryList.add(6, 6);
    claimedEntryList.add(15, 30);
    claimedEntryList.add(TTQueueNewOnVCTable.INVALID_ENTRY_ID, TTQueueNewOnVCTable.INVALID_ENTRY_ID);
    claimedEntryList.add(7, 10);
    claimedEntryList.add(1, 4);
    verifyClaimedEntryListEncode(claimedEntryList);
  }

  private void verifyClaimedEntryListEncode(ClaimedEntryList expected) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expected.encode(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    ClaimedEntryList actual = ClaimedEntryList.decode(
      new BinaryDecoder(new ByteArrayInputStream(bytes)));

    assertEquals(expected, actual);
  }

  @Test
  public void testClaimedEntryListCompare() {
    ClaimedEntryList claimedEntryList1 = new ClaimedEntryList();
    claimedEntryList1.add(2, 7);
    claimedEntryList1.add(8, 16);
    claimedEntryList1.add(25, 30);

    ClaimedEntryList claimedEntryList2 = new ClaimedEntryList();
    claimedEntryList2.add(3, 4);
    claimedEntryList2.add(7, 15);

    ClaimedEntryList claimedEntryList3 = new ClaimedEntryList();
    claimedEntryList3.add(3, 8);
    claimedEntryList3.add(15, 20);
    claimedEntryList3.add(22, 30);
    claimedEntryList3.add(10, 12);
    claimedEntryList3.add(31, 35);

    SortedSet<ClaimedEntryList> sortedSet = new TreeSet<ClaimedEntryList>();
    sortedSet.add(claimedEntryList1);
    sortedSet.add(claimedEntryList2);
    sortedSet.add(claimedEntryList3);

    assertEquals(claimedEntryList2, sortedSet.first());
    sortedSet.remove(claimedEntryList2);
    assertEquals(claimedEntryList1, sortedSet.first());
    sortedSet.remove(claimedEntryList1);
    assertEquals(claimedEntryList3, sortedSet.first());
    sortedSet.remove(claimedEntryList3);
    assertTrue(sortedSet.isEmpty());
  }

  @Test
  public void testClaimedEntryEncode() throws Exception{
    verifyClaimedEntryEncode(ClaimedEntryRange.INVALID);
    verifyClaimedEntryEncode(new ClaimedEntryRange(2, 15));
  }

  private void verifyClaimedEntryEncode(ClaimedEntryRange expected) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expected.encode(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    ClaimedEntryRange actual =
      ClaimedEntryRange.decode(new BinaryDecoder(new ByteArrayInputStream(bytes)));

    assertEquals(expected, actual);
  }

}
