/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.spark.service;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceResponder;

/**
 * A {@link HttpContentConsumer} for {@link SparkHttpServiceHandler} for large request body consumption.
 * It is more suitable for Spark handler use cases that it uses {@link TransactionControl#EXPLICIT explicit transaction}
 * for the {@link #onFinish(HttpServiceResponder)} and {@link #onError(HttpServiceResponder, Throwable)} methods.
 */
public abstract class SparkHttpContentConsumer extends HttpContentConsumer {

  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  public abstract void onFinish(HttpServiceResponder responder) throws Exception;

  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  public abstract void onError(HttpServiceResponder responder, Throwable failureCause);
}
