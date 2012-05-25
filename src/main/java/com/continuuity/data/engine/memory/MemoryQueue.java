package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.continuuity.data.operation.queue.PowerQueue;
import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;

public class MemoryQueue implements PowerQueue {

  /** Maximum default timeout of 5 minutes (can be changed by tests) */
  static long TIMEOUT = 5 * 60 * 1000;
  
  Entry head;
  Entry tail;

  /** Used for locking, generating ids, and tracking the total queue size */
  private final AtomicLong entryIds = new AtomicLong(0);

  final AtomicLong wakeUps = new AtomicLong(0);

  /** Map from GroupID to GroupHead */
  private final ConcurrentHashMap<Integer,ConsumerGroup> consumerGroups =
      new ConcurrentHashMap<Integer,ConsumerGroup>();

  public MemoryQueue() {
    this.head = null;
    this.tail = null;
  }

  @Override
  public boolean push(byte [] value) {
    Entry entry = new Entry(value, entryIds.incrementAndGet());
    synchronized(this.entryIds) {
      if (this.head == null) {
        this.head = entry;
        this.tail = entry;
      } else {
        this.tail.setNext(entry);
        this.tail = entry;
      }
      this.entryIds.notifyAll();
    }
    return true;
  }

  boolean popSync = true;
  
  @Override
  public QueueEntry pop(QueueConsumer consumer, QueueConfig config,
      boolean drain)  
  throws InterruptedException {
    // Anything in the queue at all?  Wait for a push if so
    if (head == null) {
      if (popSync) waitForPush(); else return null;
      return pop(consumer, config, drain);
    }
    
    // Determine the current state for this consumer group
    ConsumerGroup group = null;
    System.out.println("[" + consumer + "] checking " + consumer.getGroupId() + " (" + consumer.toString() + ")");
    if (!this.consumerGroups.containsKey(consumer.getGroupId())) {
      // This group has not consumed before, set group head = head
      group = new ConsumerGroup(consumer.getGroupId());
      group.setHead(head);
      ConsumerGroup existingGroup = this.consumerGroups.putIfAbsent(
          consumer.getGroupId(), group);
      if (existingGroup != null) {
        // Someone else added the group concurrently with us, use theirs
        group = existingGroup;
        System.out.println("[" + consumer + "] someone else accessing concurrently groupid=" + consumer.getGroupId() + ", now group=" + group.toString() + " (" + consumer.toString() + ")");
      } else {
        System.out.println("[" + consumer + "] inserted new group for groupid=" + consumer.getGroupId() + " (" + group.toString() + ")");
      }
    } else {
      group = this.consumerGroups.get(consumer.getGroupId());
      System.out.println("[" + consumer + "] found existing " + consumer.getGroupId() + " (" + group.toString() + ") (" + consumer.toString() + ")");
    }
    
    if (group.getId() != consumer.getGroupId()) {
      System.out.println("[" + consumer + "] ERROR!  (" + group + ") (" + consumer + ")");
    }
    
    // Iterate entries to see if we should emit one
    synchronized (group) {
      // Drain mode check
      if (drain && !group.hasDrainPoint()) {
        group.setDrainPoint(findDrainPoint(group));
      } else if (!drain && group.hasDrainPoint()) {
        group.clearDrainPoint();
      }
      Entry curEntry = group.getHead();
      while (curEntry != null && curEntry.id <= group.getDrainPoint()) {
        // Check if this is already assigned to someone else in the same group
        // or if it's already assigned to us
        GroupConsumptionInfo info =
            curEntry.getConsumerInfo(consumer.getGroupId());
        if (info.isAvailable() ||
            (info.getConsumerId() == consumer.getConsumerId() &&
              !info.isAcked() &&
              (config.isSyncMode() || drain)
              )
        ) {
          QueueEntry entry = curEntry.makeQueueEntry();
          if (config.getPartitioner().shouldEmit(consumer, entry)) {
            entry.setConsumer(consumer);
            info.setPopConsumer(consumer);
            return entry;
          }
        }
        // Entry is taken or not applicable to this consumer, iterate to next
        curEntry = curEntry.getNext();
      }
    }
      
    // Didn't find anything.  Wait and try again.
    if (popSync && !drain) waitForPush(); else return null;
    return pop(consumer, config, drain);
  }

  /**
   * Called with group already locked.
   * @param group
   * @return
   */
  private long findDrainPoint(ConsumerGroup group) {
    Entry curEntry = group.getHead();
    long drainPoint = 0;
    while (curEntry != null) {
      if (curEntry.hasGroupConsumerInfo(group.id)) {
        drainPoint = curEntry.id;
      } else {
        return drainPoint;
      }
      curEntry = curEntry.getNext();
    }
    return drainPoint;
  }

  @Override
  public boolean ack(QueueEntry entry) {
    // Queue should not be empty
    if (this.head == null) return false;
    
    QueueConsumer consumer = entry.getConsumer();

    // Group info should not be null
    ConsumerGroup group = this.consumerGroups.get(consumer.getGroupId());
    if (group == null) return false;
    
    synchronized (group) {
      Entry curEntry = group.getHead();
      while (curEntry != null) {
        // Check if this is the entry we want to ack
        GroupConsumptionInfo info =
            curEntry.getConsumerInfo(consumer.getGroupId());
        if (curEntry.id == entry.getId() &&
            info.isPopped() &&
            info.getConsumerId() == consumer.getConsumerId()) {
          // Verified it was previously popped and owned by us, ack
          info.ack();
          // See if we need to update HEAD pointer
          if (curEntry.equals(group.getHead())) {
            // We just ACKd the head, try move the head down
            curEntry = curEntry.getNext();
            while (curEntry != null) {
              group.setHead(curEntry);
              if (curEntry.getConsumerInfo(consumer.getGroupId()).isAcked()) {
                curEntry = curEntry.getNext();
              } else {
                break;
              }
            }
          }
          return true;
        }
        // Entry is taken or not applicable to this consumer, iterate to next
        curEntry = curEntry.getNext();
      }
    }
    return false;
  }

  private void waitForPush() throws InterruptedException {
    synchronized (this.entryIds) {
      long start = this.entryIds.get();
      while (this.entryIds.get() == start) {
        this.entryIds.wait();
      }
      this.wakeUps.incrementAndGet();
    }
  }

  private Random rand = new Random();

  class ConsumerGroup {
    private final int id;
    private Entry head;
    private final long uuid;
    
    private long drainPoint = Long.MAX_VALUE;
    
    ConsumerGroup(int id) {
      this.id = id;
      this.head = null;
      this.uuid = rand.nextLong();
    }
    public void setHead(Entry head) {
      this.head = head;
    }
    public Entry getHead() {
      return this.head;
    }
    int getId() {
      return this.id;
    }
    boolean hasDrainPoint() {
      return this.drainPoint != Long.MAX_VALUE;
    }
    long getDrainPoint() {
      return this.drainPoint;
    }
    void setDrainPoint(long drainPoint) {
      this.drainPoint = drainPoint;
    }
    void clearDrainPoint() {
      this.drainPoint = Long.MAX_VALUE;
    }
    @Override
    public String toString() {
      return "ConsumerGroup {" + uuid + "} id=" + id + ", head.id=" + head.getId();
    }
  }

  class Entry {
    private final byte [] value;
    private final long id;
    private Entry next;
    
    private final Map<Integer,GroupConsumptionInfo> consumers =
        new TreeMap<Integer,GroupConsumptionInfo>();

    Entry(byte [] value, long id) {
      this.value = value;
      this.id = id;
      this.next = null;
    }
    byte [] getValue() {
      return this.value;
    }
    long getId() {
      return this.id;
    }
    void setNext(Entry entry) {
      this.next = entry;
    }
    Entry getNext() {
      return this.next;
    }
    
    synchronized boolean hasGroupConsumerInfo(int groupid) {
      return this.consumers.containsKey(groupid);
    }
    
    synchronized GroupConsumptionInfo getConsumerInfo(int groupId) {
      GroupConsumptionInfo info = this.consumers.get(groupId);
      if (info == null) {
        info = new GroupConsumptionInfo();
        this.consumers.put(groupId, info);
      }
      return info;
    }
    QueueEntry makeQueueEntry() {
      return new QueueEntry(this.value, this.id);
    }
    public boolean equals(Entry other) {
      if (other.getId() == getId()) return true;
      return false;
    }
    @Override
    public String toString() {
      return "Entry id=" + id + ", next.id=" + next.getId();
    }
  }
  
  class GroupConsumptionInfo {
    private int consumerId;
    private long stamp;
    private ConsumptionState state;
    GroupConsumptionInfo() {
      this.consumerId = -1;
      this.state = ConsumptionState.AVAILABLE;
      this.updateStamp();
    }
    @Override
    public String toString() {
      return "GroupConsumptionInfo consumerId=" + consumerId +
          ", stamp=" + stamp + ", state=" + state.name();
    }
    /**
     * @return
     */
    public boolean isAcked() {
      return this.state == ConsumptionState.ACKED;
    }
    public void ack() {
      this.state = ConsumptionState.ACKED;
      updateStamp();
    }
    public boolean isPopped() {
      return this.state == ConsumptionState.POPPED;
    }
    /**
     * @param consumer
     */
    public void setPopConsumer(QueueConsumer consumer) {
      this.consumerId = consumer.getConsumerId();
      this.state = ConsumptionState.POPPED;
      updateStamp();
    }
    boolean isAvailable() {
      if (this.state == ConsumptionState.AVAILABLE ||
          this.state == ConsumptionState.TIMED_OUT) {
        return true;
      }
      if (this.state == ConsumptionState.POPPED &&
          this.stamp < (System.currentTimeMillis() - TIMEOUT)) {
        // Timed out!
        this.state = ConsumptionState.TIMED_OUT;
        return true;
      }
      return false;
    }
    public long getStamp() {
      return stamp;
    }
    public void updateStamp() {
      this.stamp = System.currentTimeMillis();
    }
    public int getConsumerId() {
      return consumerId;
    }
  }
  
  enum ConsumptionState {
    AVAILABLE, POPPED, ACKED, TIMED_OUT
  }
}
