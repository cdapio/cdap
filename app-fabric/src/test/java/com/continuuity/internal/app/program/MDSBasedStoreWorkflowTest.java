package com.continuuity.internal.app.program;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.Id;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.store.MDSBasedStore;
import com.continuuity.metadata.MetadataServiceException;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MDSBasedStoreWorkflowTest {
  private MDSBasedStore store;
  private MetaDataStore metaDataStore;

  @Before
  public void before() throws OperationException {
    metaDataStore = TestHelper.getInjector().getInstance(MetaDataStore.class);
    store = TestHelper.getInjector().getInstance(MDSBasedStore.class);

    // clean up data
    MetaDataTable mds = TestHelper.getInjector().getInstance(MetaDataTable.class);
    for (String account : mds.listAccounts(new OperationContext(DefaultId.DEFAULT_ACCOUNT_ID))) {
      mds.clear(new OperationContext(account), account, null);
    }
  }

  /**
   *
   */
  private static class SampleAppWithOneWorkflow implements Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("WorkflowTestApp")
        .setDescription("Workflow App")
        .noStream()
        .noDataSet()
        .noFlow()
        .noProcedure()
        .noBatch()
        .withWorkflow()
          .add(new WorkflowWithThreeActions())
        .build();
    }
  }

  /**
   *
   */
  private static class SampleAppWithTwoWorkflow implements Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("WorkflowTestApp")
        .setDescription("Workflow App")
        .noStream()
        .noDataSet()
        .noFlow()
        .noProcedure()
        .noBatch()
        .withWorkflow()
          .add(new WorkflowWithThreeActions())
          .add(new WorkflowWithTwoActions())
        .build();
    }
  }

  /**
   *
   */
  private static class SampleAppWithNoWorkflow implements Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("WorkflowTestApp")
        .setDescription("Workflow App")
        .noStream()
        .noDataSet()
        .noFlow()
        .noProcedure()
        .noBatch()
        .noWorkflow()
        .build();
    }
  }

  /**
   *
   */
  public static class MRJobWorkflowTest extends AbstractMapReduce {
    private final String name;

    public MRJobWorkflowTest(String name) {
      this.name = name;
    }

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName(name)
        .setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS")
        .build();
    }
  }

  /**
   *
   */
  private static class WorkflowWithThreeActions implements Workflow {
    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("Workflow1")
        .setDescription("Workflow description")
        .startWith(new MRJobWorkflowTest("wfmrjob1"))
        .then(new MRJobWorkflowTest("wfmrjob2"))
        .last(new MRJobWorkflowTest("wfmrjob3"))
        .build();
    }
  }

  /**
   *
   */
  private static class WorkflowWithTwoActions implements Workflow {
    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("Workflow2")
        .setDescription("Workflow description")
        .startWith(new MRJobWorkflowTest("wfmrjob1"))
        .last(new MRJobWorkflowTest("wfmrjob2"))
        .build();
    }
  }

  @Test
  public void testWorkflowMDSUpdateAndDeleteWorkflows()
              throws OperationException, TException, MetadataServiceException {

    //Add App with One workflow check in MDS if the workflow exists
    Id.Application id = new Id.Application(new Id.Account("wftest"), "wfapp");
    store.addApplication(id, new SampleAppWithOneWorkflow().configure(),
                        new LocalLocationFactory().create("/wfapp"));


    Assert.assertEquals(1, metaDataStore.getWorkflows("wftest").size());
    com.continuuity.metadata.types.Workflow workflow = metaDataStore.getWorkflow("wftest", "wfapp", "Workflow1");
    Assert.assertNotNull(workflow);
    Assert.assertEquals("Workflow1", workflow.getName());

    //Update the App and add another workflow check for both the workflows
    store.addApplication(id, new SampleAppWithTwoWorkflow().configure(),
                         new LocalLocationFactory().create("/wfapp"));

    Assert.assertEquals(2, metaDataStore.getWorkflows("wftest").size());
    workflow = metaDataStore.getWorkflow("wftest", "wfapp", "Workflow1");
    Assert.assertNotNull(workflow);
    Assert.assertEquals("Workflow1", workflow.getName());

    workflow = metaDataStore.getWorkflow("wftest", "wfapp", "Workflow2");
    Assert.assertNotNull(workflow);
    Assert.assertEquals("Workflow2", workflow.getName());

    //Add back the app with one workflow to see if the workflow 2 is deleted.
    store.addApplication(id, new SampleAppWithOneWorkflow().configure(),
                         new LocalLocationFactory().create("/wfapp"));


    Assert.assertEquals(1, metaDataStore.getWorkflows("wftest").size());
    workflow = metaDataStore.getWorkflow("wftest", "wfapp", "Workflow1");
    Assert.assertNotNull(workflow);
    Assert.assertEquals("Workflow1", workflow.getName());

    //Add app with no workflows and see if there is no workflow

    store.addApplication(id, new SampleAppWithNoWorkflow().configure(),
                         new LocalLocationFactory().create("/wfapp"));


    Assert.assertEquals(0, metaDataStore.getWorkflows("wftest").size());
  }
}
