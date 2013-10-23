/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.workflow.WorkflowAction;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 *
 */
public final class MapReduceWorkflowAction implements WorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceWorkflowAction.class);
  private static final String MAP_REDUCE_NAME = "mapReduceName";

  private final String name;
  private String mapReduceName;
  private Callable<MapReduceContext> mapReduceRunner;
  private WorkflowContext context;

  public MapReduceWorkflowAction(String name, String mapReduceName) {
    this.name = name;
    this.mapReduceName = mapReduceName;
  }

  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(name)
      .setDescription("Workflow action for " + mapReduceName)
      .withOptions(ImmutableMap.of(MAP_REDUCE_NAME, mapReduceName))
      .build();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;

    mapReduceName = context.getSpecification().getProperties().get(MAP_REDUCE_NAME);
    Preconditions.checkNotNull(mapReduceName, "No MapReduce name provided.");

    mapReduceRunner = context.getMapReduceRunner(mapReduceName);

    LOG.info("Initialized for MapReduce workflow action: {}", mapReduceName);
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting MapReduce workflow action: {}", mapReduceName);
      MapReduceContext mapReduceContext = mapReduceRunner.call();

      // TODO (terence) : Put something back to context.

      LOG.info("MapReduce workflow action completed: {}", mapReduceName);
    } catch (Exception e) {
      LOG.info("Failed to execute MapReduce workflow: {}", mapReduceName, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy() {
    // No-op
  }
}
