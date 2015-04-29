/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common.kafka;

import java.util.Properties;

/**
 * Decoder interface. Implementations should return a CamusWrapper with timestamp
 * set at the very least.  Camus will instantiate a descendent of this class
 * based on the property ccamus.message.decoder.class.
 * @author kgoodhop
 *
 * @param <M> The message type to be decoded
 * @param <R> The type of the decoded message
 */
public abstract class MessageDecoder<M, R> {
  protected Properties props;
  protected String topicName;

  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  public abstract CamusWrapper<R> decode(M message);
}
