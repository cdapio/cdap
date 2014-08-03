/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.examples.simplewriteandread;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
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
