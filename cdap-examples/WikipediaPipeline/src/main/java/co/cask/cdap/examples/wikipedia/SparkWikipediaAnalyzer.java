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

package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.AbstractSpark;

/**
 * Spark program that executes in a workflow and analyzes wikipedia data
 */
public class SparkWikipediaAnalyzer extends AbstractSpark {
  public static final String NAME = SparkWikipediaAnalyzer.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A Spark program that analyzes wikipedia data using Latent Dirichlet Allocation (LDA).");
    setMainClass(ScalaSparkLDA.class);
    setDriverResources(new Resources(1024));
    setExecutorResources(new Resources(1024));
  }
}
