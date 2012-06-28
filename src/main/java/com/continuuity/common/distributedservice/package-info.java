/**
 *
 * <pre>
 *   // One group specification could be provided for each flowlet (all instances can be one group)
 *   ContainerGroupSpecification cgs = new ContainerGroupSpecification.Builder()
 *                                            .setNumInstances(2)
 *                                            .setUser("user")
 *                                            .addCommand("flowlet-runner-start <flow> <flowlet-runner>")
 *                                            .create();
 *
 *   // Add the specification to application master.
 *   ApplicationMasterSpecification ams = new ApplicationMasterSpecification.Builder()
 *                                              .setConfiguration(Configuration.create())
 *                                              .setAllowedFailures(10)
 *                                              .addContainerGroupSpecification(cgs)
 *                                              .create();
 *
 *   ApplicationMasterService ams = new ApplicationMasterService(ams);
 *   ams.start();
 *   ...
 *   ...
 *   ams.stop();
 *
 * </pre>
 */
package com.continuuity.common.distributedservice;