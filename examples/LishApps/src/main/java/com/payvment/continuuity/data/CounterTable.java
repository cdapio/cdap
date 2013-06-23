/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.payvment.continuuity.Helpers;

import java.util.Map;
import java.util.TreeMap;

//import com.continuuity.api.data.util.Helpers;

/**
 * Table of counters.
 * <p/>
 * Supports two types of counters:
 * <ol>
 * <li>
 * <b>Single Key Counters</b>
 * <p/>
 * These are simple key-value counters.
 * </li>
 * <li>
 * <b>Ordered Set Counters</b>
 * <p/>
 * These map a key to an ordered set of counters.
 * <p/>
 * Counters in a set are sorted in ascending byte order.
 * </li>
 * </ol>
 * <p/>
 */
public class CounterTable extends DataSet {

  /**
   * Hard-coded column used for single key counters.
   */
  public static final byte[] COLUMN = new byte[]{'c'};

  private final Table counters;

  /**
   * Constructs a reference to a counter table by the specified name under the
   * specified flowlet context.
   */
  public CounterTable(String name) {
    super(name);
    this.counters = new Table("ct_" + getName());
  }

  public CounterTable(DataSetSpecification spec) {
    super(spec);
    this.counters = new Table(spec.getSpecificationFor("ct_" + getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .dataset(this.counters.configure())
      .create();
  }

  // Single key counter

  /**
   * Performs a synchronous read of specified counter, returning long value.
   *
   * @param counter counter name
   * @return long value, 0 if does not exist
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Long readSingleKey(String counter) throws OperationException {
    return readSingleKey(Bytes.toBytes(counter));
  }

  /**
   * Performs a synchronous read of the specified counter, returning long value.
   *
   * @param counter counter name
   * @return long value, 0 if does not exist
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Long readSingleKey(byte[] counter) throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
      this.counters.read(new Read(counter, COLUMN));
    if (result.isEmpty()) {
      return 0L;
    }
    return Bytes.toLong(result.getValue().get(COLUMN));
  }

  /**
   * Performs an asynchronous increment of the specified counter by the
   * specified amount.  Value of counter after increment is not available.
   *
   * @param counter counter name
   * @param amount  amount to increment counter by
   */
  public Long incrementSingleKey(String counter, long amount)
    throws OperationException {
    return incrementSingleKey(Bytes.toBytes(counter), amount);
  }

  /**
   * Performs an asynchronous increment of the specified counter by the
   * specified amount.  Value of counter after increment is not available.
   *
   * @param counter counter name
   * @param amount  amount to increment counter by
   */
  public Long incrementSingleKey(byte[] counter, long amount)
    throws OperationException {
    return this.counters.incrementAndGet(
      generateSingleKeyIncrement(counter, amount)).get(COLUMN);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter by the specified amount.
   * <p/>
   * See ... for more information.
   *
   * @param counter counter name
   * @param amount  amount to increment counter by
   * @return prepared increment operation
   */
  private Increment generateSingleKeyIncrement(byte[] counter, long amount) {
    return new Increment(counter, COLUMN, amount);
  }

  // Ordered counter set

  /**
   * Performs a synchronous read of the specified counter set, returning a map
   * of counter names to long values.
   *
   * @param counterSet counter set name
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<String, Long> readCounterSet(String counterSet)
    throws OperationException {
    Read read = new Read(Bytes.toBytes(counterSet));
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<String, Long> ret = new TreeMap<String, Long>();

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }

    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, returning a map
   * of counter names to long values.
   *
   * @param counterSet counter set name
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<byte[], Long> readCounterSet(byte[] counterSet)
    throws OperationException {
    Read read = new Read(counterSet);
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }

    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, finding counters
   * with names less than or equal to the specified minimum and greater than the
   * specified maximum, returning a map of counter names to long values.
   *
   * @param counterSet counter set name
   * @param minCounter minimum counter, inclusive
   * @param maxCounter maximum counter, exclusive
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<String, Long> readCounterSet(String counterSet, String minCounter,
                                          String maxCounter) throws OperationException {
    Read read = new Read(Bytes.toBytes(counterSet), Bytes.toBytes(minCounter), Bytes.toBytes(maxCounter));
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<String, Long> ret = new TreeMap<String, Long>();

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter set, finding counters
   * with names less than or equal to the specified minimum and greater than the
   * specified maximum, returning a map of counter names to long values.
   *
   * @param counterSet counter set name
   * @param minCounter minimum counter, inclusive
   * @param maxCounter maximum counter, exclusive
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<byte[], Long> readCounterSet(byte[] counterSet, byte[] minCounter,
                                          byte[] maxCounter) throws OperationException {
    Read read = new Read(counterSet, minCounter, maxCounter);
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counter in the specified
   * counter set.  Returns the long value of the counter, or 0 if not found.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @return counter long value
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Long readCounterSet(String counterSet, String counter)
    throws OperationException {
    return readCounterSet(Bytes.toBytes(counterSet), Bytes.toBytes(counter));
  }

  /**
   * Performs a synchronous read of the specified counter in the specified
   * counter set.  Returns the long value of the counter, or 0 if not found.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @return counter long value
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Long readCounterSet(byte[] counterSet, byte[] counter)
    throws OperationException {
    Read read = new Read(counterSet, counter);
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);

    if (result.isEmpty()) {
      return 0L;
    }

    return Bytes.toLong(result.getValue().get(counter));
  }

  /**
   * Performs a synchronous read of the specified counters in the specified
   * counter set.  Returns a map of counter name to counter long values for
   * all counters found.  Counters with no found value will not be in map.
   *
   * @param counterSet   counter set name
   * @param counterNames counter names
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<String, Long> readCounterSet(String counterSet,
                                          String[] counterNames) throws OperationException {
    byte[][] columns = Helpers.saToBa(counterNames);
    Read read = new Read(Bytes.toBytes(counterSet), columns);
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<String, Long> ret = new TreeMap<String, Long>();

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs a synchronous read of the specified counters in the specified
   * counter set.  Returns a map of counter name to counter long values for
   * all counters found.  Counters with no found value will not be in map.
   *
   * @param counterSet   counter set name
   * @param counterNames counter names
   * @return map of counter names to counter long values
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public Map<byte[], Long> readCounterSet(byte[] counterSet,
                                          byte[][] counterNames) throws OperationException {
    Read read = new Read(counterSet, counterNames);
    OperationResult<Map<byte[], byte[]>> result = this.counters.read(read);
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

    if (result.isEmpty()) {
      return ret;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
      ret.put(entry.getKey(), Bytes.toLong(entry.getValue()));
    }
    return ret;
  }

  /**
   * Performs an asynchronous increment of the specified counter in the
   * specified counter set by the specified amount.
   * <p/>
   * Value of counter after increment is not available.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @param amount     amount to increment counter by
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public void incrementCounterSet(String counterSet, String counter, long amount)
    throws OperationException {
    this.counters.write(generateCounterSetIncrement(counterSet, counter, amount));
  }

  /**
   * Performs an asynchronous increment of the specified counter in the
   * specified counter set by the specified amount.
   * <p/>
   * Value of counter after increment is not available.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @param amount     amount to increment counter by
   */
  public Map<byte[], Long> incrementCounterSet(byte[] counterSet,
                                               byte[] counter, long amount) throws OperationException {
    return this.counters.incrementAndGet(
      generateCounterSetIncrement(counterSet, counter, amount));
  }

  /**
   * Performs an asynchronous increment of the specified counters in the
   * specified counter set by the specified amounts.
   * <p/>
   * Values of counters after increments are not available.
   *
   * @param counterSet   counter set name
   * @param counterNames counter names
   * @param amounts      amounts to increment counters by
   */
  public Map<byte[], Long> incrementCounterSet(String counterSet,
                                               String[] counterNames, long[] amounts) throws OperationException {
    return this.counters.incrementAndGet(
      generateCounterSetIncrement(counterSet, counterNames, amounts));
  }

  /**
   * Performs an asynchronous increment of the specified counters in the
   * specified counter set by the specified amounts.
   * <p/>
   * Values of counters after increments are not available.
   *
   * @param counterSet   counter set name
   * @param counterNames counter names
   * @param amounts      amounts to increment counters by
   */
  public Map<byte[], Long> incrementCounterSet(byte[] counterSet,
                                               byte[][] counterNames, long[] amounts)
    throws OperationException {
    return this.counters.incrementAndGet(
      generateCounterSetIncrement(counterSet, counterNames, amounts));
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter in the specified counter set by the specified amount.
   * <p/>
   * See ... for more information.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @param amount     amount to increment counter by
   * @return prepared increment operation
   */
  private Increment generateCounterSetIncrement(String counterSet,
                                                String counter, long amount) {
    return new Increment(Bytes.toBytes(counterSet), Bytes.toBytes(counter), amount);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counter in the specified counter set by the specified amount.
   * <p/>
   * See ... for more information.
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @param amount     amount to increment counter by
   * @return prepared increment operation
   */
  private Increment generateCounterSetIncrement(byte[] counterSet,
                                                byte[] counter, long amount) {
    return new Increment(counterSet, counter, amount);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counters in the specified counter set by the specified amounts.
   * <p/>
   * See ... for more information.
   *
   * @param counterSet counter set name
   * @param counters   counter names
   * @param amounts    amounts to increment counters by
   * @return prepared increment operation
   */
  private Increment generateCounterSetIncrement(String counterSet,
                                                String[] counters, long[] amounts) {
    return new Increment(Bytes.toBytes(counterSet),
                         Helpers.saToBa(counters), amounts);
  }

  /**
   * Generates an increment operation that should be used to increment the
   * specified counters in the specified counter set by the specified amounts.
   * <p/>
   * See ... for more information.
   *
   * @param counterSet counter set name
   * @param counters   counter names
   * @param amounts    amounts to increment counters by
   * @return prepared increment operation
   */
  private Increment generateCounterSetIncrement(byte[] counterSet,
                                                byte[][] counters, long[] amounts) {
    return new Increment(counterSet, counters, amounts);
  }
}
