/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.cli.app;

import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fake spark program to test CLI integration with Spark
 */
public class FakeSpark extends AbstractSpark {

  public static final String NAME = "FakeSparkProgram";
  private static final Logger LOG = LoggerFactory.getLogger(FakeSpark.class);

  @Override
  public void configure() {
    setName(NAME);
    setDescription("");
    setMainClass(FakeSparkProgram.class);
  }

  public static class FakeSparkProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      // this is needed as we don't have any jobs in our spark program which can pass or fail determining the pass or
      // fail of the whole program. So we manually set the program to pass status.
      SparkProgramWrapper.setSparkProgramSuccessful(true);
      LOG.info("HelloFakeSpark");
    }
  }
}
