package com.continuuity.common.conflake;

/**
 * Is a distributed unique ID generator. The implementation is taken from
 * twitter snowflake project.
 *
 * <p>
 * Following are the properties of the IDs generated
 * <ul>
 *   <li>ID generation could be distributed across multiple data centers and multiple workers within a data center</li>
 *   <li>The numbers generated are roughly time ordered.</li>
 *   <li>The numbers are 64 bits, so they are compact and directly sortable.</li>
 * </ul>
 * </p>
 *
 */
final class UniqueIDGenerator {
  private final long twepoch = 1288834974657L;

  private final long workerIdBits = 5L;
  private final long datacenterIdBits = 5L;
  private final long sequenceBits = 12L;

  private final long workerIdShift = sequenceBits;
  private final long datacenterIdShift = sequenceBits + workerIdBits;
  private final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
  private final long sequenceMask = -1L ^ (-1L << sequenceBits);


  private final long workerId;
  private final long datacenterId;

  private long sequence;
  private long lastTimestamp = -1L;

  /**
   * Creates an instance of UniqueIDGenerator generator.
   * @param datacenterId unique id that in the range of 0-4096 (2^0 - 2^5)
   * @param workerId unique id associated with the worker serving the ids (0-4096)
   */
  public UniqueIDGenerator(long datacenterId, long workerId) {
    this.workerId = workerId;
    this.datacenterId = datacenterId;
    this.sequence = 0;
  }

  private long tilNextMillis(long lastTimestamp) {
    long timestamp = System.currentTimeMillis();
    while(timestamp <= lastTimestamp) {
      timestamp = System.currentTimeMillis();
    }
    return timestamp;
  }

  /**
   * Returns the next unique monotonically increasing number.
   * @return next unique id.
   */
  public long next() {
    long timestamp = System.currentTimeMillis();

    if(lastTimestamp == timestamp) {
      sequence = (sequence + 1) & sequenceMask;
      if(sequence == 0)  {
        timestamp = tilNextMillis(lastTimestamp);
      }
    } else {
      sequence = 0;
    }

    lastTimestamp = timestamp;

    long id  = ((timestamp - twepoch) << timestampLeftShift)
      | (datacenterId << datacenterIdShift) | (workerId << workerIdShift) | sequence;
    sequence++;
    return id;
  }

}
