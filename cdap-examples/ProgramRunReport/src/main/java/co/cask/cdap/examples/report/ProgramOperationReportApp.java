/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.examples.report;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.spark.AbstractSpark;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one service.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A service that reads the name from the KeyValueTable and responds with 'Hello [Name]!'</li>
 * </uL>
 */
public class ProgramOperationReportApp extends AbstractApplication {
  public static final String NAME = "ProgramOperationReportApp";

  @Override
  public void configure() {
    setName(NAME);
    addSpark(new ReportGenerationSparkSpecification());
  }

  /**
   * A Spark Program that uses KMeans algorithm.
   */
  public static final class ReportGenerationSparkSpecification extends AbstractSpark {

    @Override
    public void configure() {
      setName("SparkKMeansProgram");
      setDescription("Spark KMeans Program");
      setMainClass(ReportGenerationSparkScala.class);
    }
  }
}
