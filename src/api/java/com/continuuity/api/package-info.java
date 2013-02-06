package com.continuuity.api;

/**
 * Talk about Application
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
