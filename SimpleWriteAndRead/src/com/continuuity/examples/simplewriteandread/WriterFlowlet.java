package com.continuuity.examples.simplewriteandread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.examples.simplewriteandread.SimpleWriteAndReadFlow.KeyAndValue;

public class WriterFlowlet extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(WriterFlowlet.class);

  @UseDataSet(SimpleWriteAndRead.tableName)
  KeyValueTable kvTable;

  private OutputEmitter<byte[]> output;

  public void process(KeyAndValue kv) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received KeyValue " + kv);

    this.kvTable.write(
        Bytes.toBytes(kv.getKey()), Bytes.toBytes(kv.getValue()));

    output.emit(Bytes.toBytes(kv.getKey()));
  }
}
