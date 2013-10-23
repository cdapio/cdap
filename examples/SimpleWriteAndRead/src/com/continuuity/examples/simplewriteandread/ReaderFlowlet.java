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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader flowlet.
 */
public class ReaderFlowlet extends AbstractFlowlet {
  private static Logger logger = LoggerFactory.getLogger(ReaderFlowlet.class);
  @UseDataSet(SimpleWriteAndRead.TABLE_NAME)
  KeyValueTable kvTable;

  @ProcessInput
  public void process(byte[] key) {
    logger.debug(this.getContext().getName() + ": Received key " +
                Bytes.toString(key));

    // perform inline read of key and verify a value is found

    byte[] value = this.kvTable.read(key);

    if (value == null) {
      String msg = "No value found for key " + Bytes.toString(key);
      logger.error(this.getContext().getName() + msg);
      throw new RuntimeException(msg);
    }

    logger.debug(this.getContext().getName() + ": Read value (" +
                Bytes.toString(value) + ") for key (" + Bytes.toString(key) + ")");
  }
}
