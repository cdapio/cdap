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
package co.cask.cdap.examples.counttokens;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TokenCounter extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TokenCounter
                                                              .class);

  @UseDataSet(CountTokens.TABLE_NAME)
  private KeyValueTable counters;

  @ProcessInput
  public void process(String token) {
    LOG.info(this.getContext().getName() + ": Received and " +
        "incrementing count for token " + token);
    this.counters.increment(Bytes.toBytes(token), 1);
  }
}
