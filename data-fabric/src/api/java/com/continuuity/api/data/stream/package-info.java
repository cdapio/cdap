/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * Stream specification and configuration.
 *
 * Streams are used for bringing data from external systems into the Reactor.
 * Streams are identified by a string and must be explicitly created before being used.
 *
 * Streams are used along with datasets and flows to create applications. For example:
 * <blockquote>
 *   <pre>
 *     public MyApplication implements Application {
 *       public ApplicationSpecification configure() {
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
  package com.continuuity.api.data.stream;
