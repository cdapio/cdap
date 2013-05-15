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

package com.continuuity.examples.wordcount;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;

import java.util.Map;

/**
 * Counts the number of unique entries seen given any number of entries.
 */
public class UniqueCountTable extends DataSet {

  /**
   * Row and column names used for storing the unique count.
   */
  private static final byte[] UNIQUE_COUNT = Bytes.toBytes("unique");

  /**
   * Column name used for storing count of each entry.
   */
  private static final byte[] ENTRY_COUNT = Bytes.toBytes("count");
  private Table uniqueCountTable;
  private Table entryCountTable;

  public UniqueCountTable(String name) {
    super(name);
    this.uniqueCountTable = new Table("unique_count_" + name);
    this.entryCountTable = new Table("entry_count_" + name);
  }

  public UniqueCountTable(DataSetSpecification spec) {
    super(spec);
    this.uniqueCountTable = new Table(
      spec.getSpecificationFor("unique_count_" + this.getName()));
    this.entryCountTable = new Table(
      spec.getSpecificationFor("entry_count_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .dataset(this.uniqueCountTable.configure())
      .dataset(this.entryCountTable.configure())
      .create();
  }

  /**
   * Returns the current unique count.
   *
   * @return current number of unique entries
   * @throws OperationException
   */
  public Long readUniqueCount() throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
      this.uniqueCountTable.read(new Read(UNIQUE_COUNT, UNIQUE_COUNT));

    if (result.isEmpty()) {
      return 0L;
    }

    byte[] countBytes = result.getValue().get(UNIQUE_COUNT);

    if (countBytes == null || countBytes.length != 8) {
      return 0L;
    }

    return Bytes.toLong(countBytes);
  }

  /**
   * Adds the specified entry to the table and augments the specified Tuple with
   * a special field that will be used in the downstream Flowlet this Tuple is
   * sent to.
   * Continuously add entries into the table using this method, pass the Tuple
   * to another downstream Flowlet, and in the second Flowlet pass the Tuple to
   * the {@link #updateUniqueCount(String)}.
   *
   * @param entry entry to add
   * @throws OperationException
   */
  public void updateUniqueCount(String entry)
    throws OperationException {
    Long newCount = this.entryCountTable.
      incrementAndGet(new Increment(Bytes.toBytes(entry), ENTRY_COUNT, 1L)).
      get(ENTRY_COUNT);
    if (newCount == 1L) {
      this.uniqueCountTable.write(
        new Increment(UNIQUE_COUNT, UNIQUE_COUNT, 1L));
    }
  }
}
