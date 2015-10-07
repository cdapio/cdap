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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.Dataset;

import java.nio.ByteBuffer;

/**
 * A {@link HttpContentConsumer} that supports non-transactional processing of HTTP request body.
 * One can extend from this class and implement the {@link #onReceived(ByteBuffer, Transactional)} method
 * to achieve better performance when receiving data from the client.
 */
public abstract class NonTransactionalHttpContentConsumer extends HttpContentConsumer {

  @Override
  public final void onReceived(ByteBuffer chunk) throws Exception {
    throw new UnsupportedOperationException("onReceived without transaction executor is not supported.");
  }

  /**
   * This method will get invoked when a new chunk of the request body is available to be consumed.
   * It is guaranteed that no concurrent calls to this method will be made.
   * <p>
   * Access to transactional {@link Dataset} can be done through the {@link Transactional#execute(TxRunnable)} method.
   * If access to transactional {@link Dataset} is always needed, it is better to extend from the
   * {@link HttpContentConsumer} class instead.
   * </p>
   *
   * @param chunk a {@link ByteBuffer} containing a chunk of the request body
   * @param transactional for executing a {@link TxRunnable} in a single transaction.
   * @throws Exception if there is any error when processing the received chunk
   */
  public abstract void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception;
}
