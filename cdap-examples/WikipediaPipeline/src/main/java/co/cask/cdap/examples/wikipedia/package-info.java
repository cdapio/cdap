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

/**
 * This package contains the WikipediaPipeline Application that demonstrates a CDAP Workflow for processing and
 * analyzing Wikipedia data.
 * <p>
 *   The app contains a CDAP Workflow that runs in either online or offline mode.
 *   In the offline mode, it expects Wikipedia data to be available in a Stream.
 *   In the online mode, it attempts to download wikipedia data for a provided set of page titles
 *   (formatted as the output of the Facebook Likes API). Once wikipedia data is available it runs a map-only job to
 *   filter bad records and normalize data formatted as text/wiki-text into text/plain.
 *
 *   It then runs two analyses on the plain text data in a fork:
 * </p>
 *
 * <ol>
 *   <li>
 *   {@link co.cask.cdap.examples.wikipedia.ScalaSparkLDA} runs topic modeling on Wikipedia data using Latent
 *   Dirichlet Allocation (LDA).
 *   </li>
 *   <li>
 *   {@link co.cask.cdap.examples.wikipedia.TopNMapReduce} that produces the Top N terms in the supplied Wikipedia
 *   data.
 *   </li>
 *   <li>
 *   The output of the above analyses is stored in the following datasets:
 *     <ul>
 *       <li>A Table named lda which contains the output of the Spark LDA program.</li>
 *       <li>A KeyValueTable named topn which contains the output of the TopNMapReduce program.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>
 *   One of the main purposes of this application is to demonstrate how the flow of a typical data pipeline can be
 *   controlled using Workflow Tokens.
 * </p>
 */
package co.cask.cdap.examples.wikipedia;
