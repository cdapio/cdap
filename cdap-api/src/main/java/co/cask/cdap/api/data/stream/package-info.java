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

/**
 * Stream specification and configuration.
 *
 * Streams are used for bringing data from external systems into CDAP.
 * Streams are identified by a string and must be explicitly created before being used.
 *
 * Streams are used along with datasets and flows to create applications. For example:
 * <blockquote>
 *   <pre>
 *     public MyApplication extends AbstractApplication {
 *       public void configure() {
 *         MyDataSet myDataset = new MyDataset("my");
 *         TimeseriesDataSet timeseriesDataset = new TimeseriesDataSet("mytimeseries");
 *         Stream clickStream = new Stream("mystream");
 *         return new ApplicationSpecification.Builder()
 *            .addDataSet(myDataset)
 *            .addDataSet(timeseriesDataset);
 *            .addStream(clickStream)
 *            .addFlow(ClickProcessingFlow.class)
 *            .create();
 *       }
 *     }
 *   </pre>
 * </blockquote>
 *
 */
  package co.cask.cdap.api.data.stream;
