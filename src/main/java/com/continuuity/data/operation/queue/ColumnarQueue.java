package com.continuuity.data.operation.queue;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.table.ColumnarTable;

public class ColumnarQueue implements PowerQueue {

  private static final byte [] COUNT_COL = new byte [] { 0 , 1 };

  private static final byte [] GROUP_INFO_PREFIX = new byte [] { 0 , 2 };

  private static final byte [] ENTRY_PREFIX = new byte [] { 3 , 1 };

  private final ColumnarTable table;
  private final byte [] key;

  public ColumnarQueue(ColumnarTable table, byte [] key) {
    this.table = table;
    this.key = key;
  }

  @Override
  public boolean push(byte[] value) {
    long entryId = this.table.increment(this.key, COUNT_COL, 1);
//    this.table.put(this.key, makeEntryCol(entryId), value);
    return true;
  }

  @Override
  public QueueEntry pop(QueueConsumer consumer, QueueConfig config,
      boolean drain) throws InterruptedException {
    // Upsert group info
    GroupInfo groupInfo = GroupInfo.upsert(this.table, this.key,
        consumer, config, drain);
    // Drain mode checks
    if (drain && !groupInfo.hasDrainPoint()) {
      groupInfo.setDrainPoint(findDrainPoint(groupInfo));
    } else if (!drain && groupInfo.hasDrainPoint()) {
      groupInfo.clearDrainPoint();
    }
    // Iterate entries from head
    Entry curEntry = Entry.getNext(this.table, this.key, groupInfo.headId);
//    while (curEntry != null && curEntry.id <= groupInfo.getDrainPoint()) {
//      
//      
//      curEntry = getNextEntry(curEntry.id);
//    }
    return null;
  }

  private long findDrainPoint(GroupInfo groupInfo) {
    // TODO: implement correctly
    return this.table.increment(this.key, COUNT_COL, 0);
  }

  static class Entry {

    public static Entry getNext(ColumnarTable table, byte[] key,
        long previousId) {
//      table.get(key, startColumn, stopColumn, 1);
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  static class GroupInfo {

    static final byte [] GROUP_ID = new byte [] { 0 , 1 };
    static final byte [] HEAD_ID = new byte [] { 0 , 2 };
    static final byte [] DRAIN_POINT = new byte [] { 0 , 3 };

    long groupId;
    long headId;
    long drainPoint;

    private GroupInfo(long groupId, long headId, long drainPoint) {
      this.groupId = groupId;
      this.headId = headId;
      this.drainPoint = drainPoint;
    }

    public void setDrainPoint(long drainPoint) {
      this.drainPoint = drainPoint;
    }

    public void clearDrainPoint() {
      setDrainPoint(Long.MAX_VALUE);
    }

    public boolean hasDrainPoint() {
      return getDrainPoint() != Long.MAX_VALUE;
    }

    public long getDrainPoint() {
      return this.drainPoint;
    }

    static GroupInfo upsert(ColumnarTable table, byte [] key,
        QueueConsumer consumer, QueueConfig config, boolean drain) {
      byte [][] columns = new byte [][] {
          makeGroupInfoCol(consumer.getGroupId(), GROUP_ID),
          makeGroupInfoCol(consumer.getGroupId(), HEAD_ID),
          makeGroupInfoCol(consumer.getGroupId(), DRAIN_POINT)
      };
      Map<byte[],byte[]> map = table.get(key, columns);
      if (map == null || map.isEmpty()) {
        // Group hasn't been initialized, initialize
        byte [][] values = new byte [][] {
            Bytes.toBytes(consumer.getGroupId()),
            Bytes.toBytes(0L),
            Bytes.toBytes(Long.MAX_VALUE)
        };
        table.put(key, columns, values);
        return new GroupInfo(consumer.getGroupId(), 0L, Long.MAX_VALUE);
      }
      assert(map.size() == 3);
      assert(consumer.getGroupId() == Bytes.toLong(map.get(columns[0])));
      return new GroupInfo(
          Bytes.toLong(map.get(columns[0])),
          Bytes.toLong(map.get(columns[1])),
          Bytes.toLong(map.get(columns[2])));
    }
  }

  @Override
  public boolean ack(QueueEntry entry) {
    // TODO Auto-generated method stub
    return false;
  }

  // Private helpers

  private static byte[] makeEntryCol(long entryId, byte [] suffix) {
    return Bytes.add(ENTRY_PREFIX, Bytes.toBytes(entryId), suffix);
  }

  private static byte [] makeGroupInfoCol(long groupId, byte [] suffix) {
    return Bytes.add(GROUP_INFO_PREFIX, Bytes.toBytes(groupId), suffix);
  }
}
