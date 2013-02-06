package com.continuuity.api;

/**
 * Application is a logical grouping of Streams, Datasets, Flows & Procedures that is deployable.
 *
 * <p>
 *   To create an application, one need to implement the interface {@code Application}. Within the
 *   Application's configure you will create all the different entities that are needed to form an
 *   application.
 * </p>
 *
 * <p>
 * Example usage
 *   <pre>
 *   public MyApplication implements Application {
 *
 *     public ApplicationSpecification configure() {
 *
 *       return ApplicationSpecification.builder()
 *             .setName("myFirstApp")
 *             .setDescription("This is my first application")
 *             .withStream().add(new Stream("text"))
 *                          .add(new Stream("log"))
 *             .withDataSet().add(new KeyValueTable("mytable"))
 *                           .add(new SimpleTimeseriesTable("tstable"))
 *             .withFlow().add(new MyFirstFlow())
 *                        .add(new LogProcessFlow())
 *             .withProcedure().add(new KeyValueLookupProcedure())
 *                             .add(new LogProcedure())
 *             .build();
 *     }
 *   }
 *   </pre>
 * </p>
 */
