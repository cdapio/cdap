/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;

import java.util.Map;

/**
 * Reactor Application Specification used in core code
 */
public interface ApplicationSpecification {

  /**
   * @return Name of the Application.
   */
  String getName();

  /**
   * @return Description of the Application.
   */
  String getDescription();

  /**
   * @return An immutable {@link Map} from {@link Stream} name to {@link StreamSpecification}
   *         for {@link Stream}s configured for the Application.
   */
  Map<String, StreamSpecification> getStreams();

  /**
   * @return An immutable {@link Map} from {@link DataSet} name to {@link DataSetSpecification}
   *         for {@link DataSet}s configured for the Application.
   */
  Map<String, DataSetSpecification> getDataSets();

  /**
   * @return An immutable {@link Map} from {@link Flow} name to {@link FlowSpecification}
   *         for {@link Flow}s configured for the Application.
   */
  Map<String, FlowSpecification> getFlows();

  /**
   * @return An immutable {@link Map} from {@link Procedure} name to {@link ProcedureSpecification}
   *         for {@link Procedure}s configured for the Application.
   */
  Map<String, ProcedureSpecification> getProcedures();

  /**
   * @return An immutable {@link Map} from {@link MapReduce} name to {@link MapReduceSpecification}
   *         for {@link MapReduce} jobs configured for the Application.
   */
  Map<String, MapReduceSpecification> getMapReduce();

  /**
   * @return An immutable {@link Map} from {@link Workflow} name to {@link WorkflowSpecification}
   *         for {@link Workflow}s configured for the Application.
   */
  Map<String, WorkflowSpecification> getWorkflows();
}
