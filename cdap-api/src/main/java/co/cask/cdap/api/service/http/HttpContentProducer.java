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
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.dataset.Dataset;

import java.nio.ByteBuffer;

/**
 * Instance of this class is for producing an HTTP response body in small chunks to avoid running out of memory
 * when responding with a large body. An instance of this class can be provided to one of the {@code send}
 * methods in {@link HttpServiceResponder}.
 */
public abstract class HttpContentProducer {

  /**
   * Returns the length of the content or {@code -1L} if the length is unknown.
   */
  public long getContentLength() {
    return -1L;
  }

  /**
   * This method provides a new chunk of data to be sent to the client.
   * It is guaranteed that no concurrent calls to this method will be made.
   * The implementation can reuse the same {@link ByteBuffer} instance
   * across multiple calls to this method.
   *
   * <p>
   * Access to transactional {@link Dataset Datasets} must be done through the
   * {@link Transactional#execute(TxRunnable)} method.
   * </p>
   *
   * @param transactional for executing a {@link TxRunnable} in a single transaction.
   * @return a {@link ByteBuffer} containing the next chunk of bytes to be sent to the client.
   *         If the returned {@link ByteBuffer} is empty ({@link ByteBuffer#hasRemaining()} == {@code false}),
   *         it signals that's the end of the response body.
   * @throws Exception if there is any error
   */
  public abstract ByteBuffer nextChunk(Transactional transactional) throws Exception;

  /**
   * This method will get invoked after the last chunk of data is sent. It is always
   * executed inside a single transaction unless annotated with {@link TransactionPolicy(TransactionControl}.
   *
   * @throws Exception if there is any error
   */
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public abstract void onFinish() throws Exception;

  /**
   * This method will get invoked when there is an error while sending body chunks.
   *
   * Any issues related to network as well as any {@link Exception Exceptions} raised
   * from either {@link #nextChunk(Transactional)}
   * or {@link #onFinish()} methods will have this method invoked.
   *
   * This method is always executed inside a single transaction unless annotated
   * with {@link TransactionPolicy(TransactionControl}.
   *
   * @param failureCause the reason of the failure
   */
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public abstract void onError(Throwable failureCause);
}
