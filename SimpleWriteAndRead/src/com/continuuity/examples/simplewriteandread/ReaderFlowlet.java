package com.continuuity.examples.simplewriteandread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class ReaderFlowlet extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(ReaderFlowlet.class);

  @UseDataSet(SimpleWriteAndRead.tableName)
  KeyValueTable kvTable;

  public void process(byte[] key) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received key " +
        Bytes.toString(key));

    // perform inline read of key and verify a value is found

    byte [] value = this.kvTable.read(key);

    if (value == null) {
      String msg = "No value found for key " + Bytes.toString(key);
      LOG.error(this.getContext().getName() + msg);
      throw new RuntimeException(msg);
    }

    LOG.debug(this.getContext().getName() + ": Read value (" +
        Bytes.toString(value) + ") for key (" + Bytes.toString(key) + ")");
  }
}