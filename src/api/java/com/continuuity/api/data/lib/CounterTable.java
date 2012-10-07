package com.continuuity.api.data.lib;

import java.util.Map;
import java.util.TreeMap;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.Read;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.util.Bytes;
import com.continuuity.api.data.util.Helpers;

/**
 * Table of counters.
 * <p>
 * Supports two types of counters:
 * <ol>
 *  <li>
 *   <b>Single Key Counters</b>
 *   <p>
 *   These are simple key-value counters.
 *  </li>
 *  <li>
 *   <b>Ordered Set Counters</b>
 *   <p>
 *   These map a key to an ordered set of counters.
 *   <p>
 *   Counters in a set are sorted in ascending byte order.
 *  </li>
 * </ol>
 * <p>
 */
public class CounterTable extends DataLib {

  /** Hard-coded column used for single key counters */
  public static final byte [] COLUMN = new byte [] { 'c' };

  /**
   * Constructs a reference to a counter table by the specified name under the
   * specified flowlet context.
   * @param tableName
   * @param fabric 
   * @param registry 
   */
  public CounterTable(String tableName, DataFabric fabric,
      BatchCollectionRegistry registry) {
    super(tableName, fabric, registry);
  }

  // Single key counter

  /**
   * Performs a synchronous read of specified counter, returning long value.
   * @param counter counter name
   * @return long value, 0 if does not exist
   * @throws OperationException
   */
  public Long readSingleKey(String counter) throws OperationException {
    return readSingleKey(Bytes.toBytes(counter));
  }

  /**
   * Performs a synchronous read of the specified counter, returning long value.
   * @param counter counter name
   * @return long value, 0 if does not exist
   * @throws OperationException
   */
  public Long readSingleKey(byte [] counter) throws OperationException {
    Read read = new Read(this.tableName, counter, COLUMN);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    if (result.isEmpty()) return 0L;
    return Bytes.toLong(result.getValue().get(COLUMN));
  }

  /**
   * Performs an asynchronous increment of the specified counter by the
   * specified amount.  Value of counter after increment is not available.
   * @param counter counter name
   * @param amount amount to increment counter by
   */
  public void incrementSingleKey(String counter, long amount) {
    incrementSingleKey(Bytes.toBytes(counter), amount);
  }

  /**
   * Performs an asynchronous increment of the specified counter by the
   * specified amount.  Value of counter after increment is not available.
   * @param counter counter name
   * @param amount amount to increment counter by
   */
  public void incrementSingleKey(byte [] counter, long amount) {
    this.collector.add(generateSingleKeyIncrement(counter, amount));
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter by the specified amount.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counter counter name
   * @param amount amount to increment counter by
   * @return prepared increment operation
   */
  public Increment generateSingleKeyIncrement(String counter, long amount) {
    return generateSingleKeyIncrement(Bytes.toBytes(counter), amount);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter by the specified amount.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counter counter name
   * @param amount amount to increment counter by
   * @return prepared increment operation
   */
  public Increment generateSingleKeyIncrement(byte [] counter, long amount) {
    return new Increment(this.tableName, counter, COLUMN, amount);
  }

  // Ordered counter set

  /**
   * Performs a synchronous read of the specified counter set, returning a map
   * of counter names to long values.
   * @param counterSet counter set name
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<String,Long> readCounterSet(String counterSet)
      throws OperationException {
    Read read = new Read(this.tableName, Bytes.toBytes(counterSet));
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<String,Long> ret = new TreeMap<String,Long>();
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, returning a map
   * of counter names to long values.
   * @param counterSet counter set name
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<byte[],Long> readCounterSet(byte [] counterSet)
      throws OperationException {
    Read read = new Read(this.tableName, counterSet);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, finding counters
   * with names less than or equal to the specified minimum and greater than the
   * specified maximum, returning a map of counter names to long values.
   * @param counterSet counter set name
   * @param minCounter minimum counter, inclusive
   * @param maxCounter maximum counter, exclusive
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<String,Long> readCounterSet(String counterSet, String minCounter,
      String maxCounter) throws OperationException {
    ReadColumnRange read = new ReadColumnRange(this.tableName,
        Bytes.toBytes(counterSet), Bytes.toBytes(minCounter),
        Bytes.toBytes(maxCounter));
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<String,Long> ret = new TreeMap<String,Long>();
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, finding counters
   * with names less than or equal to the specified minimum and greater than the
   * specified maximum, returning a map of counter names to long values.
   * @param counterSet counter set name
   * @param minCounter minimum counter, inclusive
   * @param maxCounter maximum counter, exclusive
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<byte[],Long> readCounterSet(byte [] counterSet, byte [] minCounter,
      byte [] maxCounter) throws OperationException {
    ReadColumnRange read = new ReadColumnRange(this.tableName, counterSet,
        minCounter, maxCounter);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter in the specified
   * counter set.  Returns the long value of the counter, or 0 if not found.
   * @param counterSet counter set name
   * @param counter counter name
   * @return counter long value
   * @throws OperationException
   */
  public Long readCounterSet(String counterSet, String counter)
  throws OperationException {
    return readCounterSet(Bytes.toBytes(counterSet), Bytes.toBytes(counter));
  }

  /**
   * Performs a synchronous read of the specified counter in the specified
   * counter set.  Returns the long value of the counter, or 0 if not found.
   * @param counterSet counter set name
   * @param counter counter name
   * @return counter long value
   * @throws OperationException
   */
  public Long readCounterSet(byte [] counterSet, byte [] counter)
  throws OperationException {
    Read read = new Read(this.tableName, counterSet, counter);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    if (result.isEmpty()) return 0L;
    return Bytes.toLong(result.getValue().get(counter));
  }

  /**
   * Performs a synchronous read of the specified counters in the specified
   * counter set.  Returns a map of counter name to counter long values for
   * all counters found.  Counters with no found value will not be in map.
   * @param counterSet counter set name
   * @param counters counter names
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<String,Long> readCounterSet(String counterSet,
      String [] counters) throws OperationException {
    byte [][] columns = Helpers.saToBa(counters);
    Read read = new Read(this.tableName, Bytes.toBytes(counterSet), columns);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<String,Long> ret = new TreeMap<String,Long>();
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counters in the specified
   * counter set.  Returns a map of counter name to counter long values for
   * all counters found.  Counters with no found value will not be in map.
   * @param counterSet counter set name
   * @param counters counter names
   * @return map of counter names to counter long values
   * @throws OperationException
   */
  public Map<byte[],Long> readCounterSet(byte [] counterSet,
      byte [][] counters) throws OperationException {
    Read read = new Read(this.tableName, counterSet, counters);
    OperationResult<Map<byte[],byte[]>> result = this.fabric.read(read);
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    if (result.isEmpty()) return ret;
    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs an asynchronous increment of the specified counter in the
   * specified counter set by the specified amount.
   * <p>
   * Value of counter after increment is not available.
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount amount to increment counter by
   * @throws OperationException
   */
  public void incrementCounterSet(String counterSet, String counter,
      long amount) throws OperationException {
    this.collector.add(
        generateCounterSetIncrement(counterSet, counter, amount));
  }

  /**
   * Performs an asynchronous increment of the specified counter in the
   * specified counter set by the specified amount.
   * <p>
   * Value of counter after increment is not available.
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount amount to increment counter by
   */
  public void incrementCounterSet(byte [] counterSet, byte [] counter,
      long amount) {
    this.collector.add(
        generateCounterSetIncrement(counterSet, counter, amount));
  }

  /**
   * Performs an asynchronous increment of the specified counters in the
   * specified counter set by the specified amounts.
   * <p>
   * Values of counters after increments are not available.
   * @param counterSet counter set name
   * @param counters counter names
   * @param amounts amounts to increment counters by
   */
  public void incrementCounterSet(String counterSet,
      String [] counters, long [] amounts) {
    this.collector.add(
        generateCounterSetIncrement(counterSet, counters, amounts));
  }

  /**
   * Performs an asynchronous increment of the specified counters in the
   * specified counter set by the specified amounts.
   * <p>
   * Values of counters after increments are not available.
   * @param counterSet counter set name
   * @param counters counter names
   * @param amounts amounts to increment counters by
   */
  public void incrementCounterSet(byte [] counterSet,
      byte [][] counters, long [] amounts) {
    this.collector.add(
        generateCounterSetIncrement(counterSet, counters, amounts));
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter in the specified counter set by the specified amount.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount amount to increment counter by
   * @return prepared increment operation
   */
  public Increment generateCounterSetIncrement(String counterSet,
      String counter, long amount) {
    return new Increment(this.tableName, Bytes.toBytes(counterSet),
        Bytes.toBytes(counter), amount);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter in the specified counter set by the specified amount.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount amount to increment counter by
   * @return prepared increment operation
   */
  public Increment generateCounterSetIncrement(byte [] counterSet,
      byte [] counter, long amount) {
    return new Increment(this.tableName, counterSet, counter, amount);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counters in the specified counter set by the specified amounts.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counterSet counter set name
   * @param counters counter names
   * @param amounts amounts to increment counters by
   * @return prepared increment operation
   */
  public Increment generateCounterSetIncrement(String counterSet,
      String [] counters, long [] amounts) {
    return new Increment(this.tableName, Bytes.toBytes(counterSet),
        Helpers.saToBa(counters), amounts);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counters in the specified counter set by the specified amounts.
   * <p>
   * This increment can be performed synchronously using
   * {@link DataFabric#execute(Increment)} or performed asynchronously with the
   * final counter value returned as a field in a tuple.
   * <p>
   * See ... for more information.
   * @param counterSet counter set name
   * @param counters counter names
   * @param amounts amounts to increment counters by
   * @return prepared increment operation
   */
  public Increment generateCounterSetIncrement(byte [] counterSet,
      byte [][] counters, long [] amounts) {
    return new Increment(this.tableName, counterSet, counters, amounts);
  }
}
