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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;

/**
 * Spark Programs that are used to test explicit transactions in Spark lifecycle.
 * TODO: These is cannot be static inner classes of the {@link AppWithCustomTx} due to CDAP-7428
 */
public class SparkWithCustomTx {

  public static class TxSpark extends AbstractSpark implements JavaSparkMain {
    @Override
    protected void configure() {
      setName(AppWithCustomTx.SPARK_TX);
      setMainClass(TxSpark.class);
    }

    @Override
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      AppWithCustomTx.recordTransaction(getContext(), AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE);
      AppWithCustomTx.attemptNestedTransaction(getContext(), AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      AppWithCustomTx.recordTransaction(getContext(), AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY);
      AppWithCustomTx.attemptNestedTransaction(getContext(), AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY_NEST);
    }

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      // no-op
    }
  }

  public static class NoTxSpark extends TxSpark {
    @Override
    protected void configure() {
      super.configure();
      setName(AppWithCustomTx.SPARK_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      AppWithCustomTx.recordTransaction(getContext(), AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE);
      AppWithCustomTx.executeRecordTransaction(getContext(), AppWithCustomTx.SPARK_NOTX,
                                               AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_SPARK_INITIALIZE);
      AppWithCustomTx.executeAttemptNestedTransaction(getContext(),
                                                      AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      AppWithCustomTx.recordTransaction(getContext(), AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY);
      AppWithCustomTx.executeRecordTransaction(getContext(), AppWithCustomTx.SPARK_NOTX,
                                               AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_SPARK_DESTROY);
      AppWithCustomTx.executeAttemptNestedTransaction(getContext(),
                                                      AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_NEST);
    }
  }
}
