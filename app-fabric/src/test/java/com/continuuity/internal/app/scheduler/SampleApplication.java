package com.continuuity.internal.app.scheduler;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.schedule.Schedule;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.internal.schedule.DefaultSchedule;
import com.google.common.base.Throwables;

/**
 *  Sample application to test if the scheduler has run the map-reduce job.
 *  MR job does nothing but sets a global flag to indicate the job has run.
 */
public class SampleApplication implements Application {

  public static int hasRun = 0;

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("SampleApp")
        .setDescription("Sample application")
        .noStream()
        .withDataSets()
          .add(new ObjectStore<String>("input", String.class))
          .add(new ObjectStore<String>("output", String.class))
        .noFlow()
        .noProcedure()
        .noBatch()
        .withWorkflow()
        .add(new SampleWorkflow())
        .build();
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sample workflow. Schedules a dummy MR job.
   */
  public static class SampleWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("SampleWorkflow")
        .setDescription("SampleWorkflow description")
        .startWith(new DummyMR())
        .last(new DummyMR())
        .addSchedule(new DefaultSchedule("Schedule", "Run every 1 minutes", "* * * * *",
                                         Schedule.Action.START))
        .build();
    }
  }

  /**
   * Dummy mapreduce job. Sets a flag in initialization.
   */
  public static class DummyMR extends AbstractMapReduce {
   @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("SimpleMapreduce")
        .setDescription("Mapreduce that does nothing")
        .useInputDataSet("input")
        .useOutputDataSet("output")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      SampleApplication.hasRun = 1;
    }
  }
}
