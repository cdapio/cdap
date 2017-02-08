/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.common.AbstractStageContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.tephra.TransactionFailureException;

/**
 * Default implementation of StreamingContext for Spark.
 */
public class DefaultStreamingContext extends AbstractStageContext implements StreamingContext {
  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext jsc;

  public DefaultStreamingContext(String stageName, JavaSparkExecutionContext sec, JavaStreamingContext jsc) {
    super(sec.getPluginContext(), sec.getMetrics(), stageName);
    this.sec = sec;
    this.jsc = jsc;
  }

  @Override
  public JavaStreamingContext getSparkStreamingContext() {
    return jsc;
  }

  @Override
  public JavaSparkExecutionContext getSparkExecutionContext() {
    return sec;
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    sec.execute(runnable);
  }

  @Override
  public void execute(int timeout, TxRunnable runnable) throws TransactionFailureException {
    sec.execute(timeout, runnable);
  }
}
