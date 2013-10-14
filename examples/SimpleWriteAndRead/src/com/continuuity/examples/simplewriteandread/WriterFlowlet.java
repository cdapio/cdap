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

package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.examples.simplewriteandread.SimpleWriteAndReadFlow.KeyAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer flowlet.
 */
public class WriterFlowlet extends AbstractFlowlet {
  private static Logger logger = LoggerFactory.getLogger(WriterFlowlet.class);
  @UseDataSet(SimpleWriteAndRead.TABLE_NAME)
  KeyValueTable kvTable;
  private OutputEmitter<byte[]> output;

  @ProcessInput
  public void process(KeyAndValue kv) {
    logger.debug(this.getContext().getName() + ": Received KeyValue " + kv);

    this.kvTable.write(
      Bytes.toBytes(kv.getKey()), Bytes.toBytes(kv.getValue()));

    output.emit(Bytes.toBytes(kv.getKey()));
  }
}
