/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectStores;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.Value;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.app.runtime.batch.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * App with workflow.
 */
@Requirements(capabilities = "cdc")
public class AppWithWorkflow extends AbstractApplication {
  public static final String NAME = "AppWithWorkflow";

  @Override
  public void configure() {
    try {
      setName(NAME);
      setDescription("Sample application");
      ObjectStores.createObjectStore(getConfigurer(), "input", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "output", String.class);
      addMapReduce(new WordCountMapReduce());
      addWorkflow(new SampleWorkflow());
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sample workflow. has a dummy action.
   */
  public static class SampleWorkflow extends AbstractWorkflow {
    public static final String NAME = "SampleWorkflow";
    public static final String FIRST_ACTION = "firstAction";
    public static final String SECOND_ACTION = "secondAction";
    public static final String WORD_COUNT_MR = WordCountMapReduce.class.getSimpleName();
    public static final String TABLE_NAME = "MyTable";
    public static final String FILE_NAME = "MyFile";
    public static final String INITIALIZE_TOKEN_KEY = "workflow.initialize.key";
    public static final String INITIALIZE_TOKEN_VALUE = "workflow.initialize.value";
    public static final String DESTROY_TOKEN_KEY = "workflow.destroy";
    public static final String DESTROY_TOKEN_SUCCESS_VALUE = "workflow.destroy.success";
    public static final String DESTROY_TOKEN_FAIL_VALUE = "workflow.destroy.fail";

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      context.getToken().put(SampleWorkflow.INITIALIZE_TOKEN_KEY, SampleWorkflow.INITIALIZE_TOKEN_VALUE);
    }

    @Override
    public void destroy() {
      WorkflowToken token = getContext().getToken();
      @SuppressWarnings("ConstantConditions")
      String initializeValue = token.get(SampleWorkflow.INITIALIZE_TOKEN_KEY, SampleWorkflow.NAME).toString();
      if (!initializeValue.equals(SampleWorkflow.INITIALIZE_TOKEN_VALUE)) {
        // Should not happen, since we are always putting token in the Workflow.initialize method.
        // We can not throw exception here since any exception thrown will be caught in the Workflow driver.
        // So in order to test this put some token value which is check in the test case.
        token.put(SampleWorkflow.DESTROY_TOKEN_KEY, SampleWorkflow.DESTROY_TOKEN_FAIL_VALUE);
      } else {
        token.put(SampleWorkflow.DESTROY_TOKEN_KEY, SampleWorkflow.DESTROY_TOKEN_SUCCESS_VALUE);
      }
    }

    @Override
    public void configure() {
      setName(NAME);
      setDescription("SampleWorkflow description");
      createLocalDataset(TABLE_NAME, KeyValueTable.class, DatasetProperties.builder().add("foo", "bar").build());
      createLocalDataset(FILE_NAME, FileSet.class, DatasetProperties.builder().add("anotherFoo", "anotherBar").build());
      addAction(new DummyAction(FIRST_ACTION));
      addAction(new DummyAction(SECOND_ACTION));
      addMapReduce(WORD_COUNT_MR);
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);
    public static final String TOKEN_KEY = "tokenKey";
    public static final String TOKEN_VALUE = "tokenValue";
    private final String name;

    public DummyAction(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
    }

    @Override
    public void initialize() throws Exception {
      WorkflowToken workflowToken = getContext().getWorkflowToken();
      workflowToken.put(TOKEN_KEY, TOKEN_VALUE);
    }

    @Override
    public void run() {
      LOG.info("Ran dummy action");
      @SuppressWarnings("ConstantConditions")
      String initializeValue = getContext().getWorkflowToken().get(SampleWorkflow.INITIALIZE_TOKEN_KEY,
                                                                   SampleWorkflow.NAME).toString();
      if (!initializeValue.equals(SampleWorkflow.INITIALIZE_TOKEN_VALUE)) {
        String msg = String.format("Expected value of token %s but got %s.", SampleWorkflow.INITIALIZE_TOKEN_VALUE,
                                   initializeValue);
        throw new IllegalStateException(msg);
      }
    }
  }

  /**
   * WordCount MapReduce job
   */
  public static final class WordCountMapReduce extends AbstractMapReduce {
    public static final String NAME = "WordCountMapReduce";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("WordCount job from Hadoop examples");
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();
      String inputPath = args.get("inputPath");
      String outputPath = args.get("outputPath");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
      WorkflowToken workflowToken = context.getWorkflowToken();
      if (workflowToken == null) {
        return;
      }
      // Put something in the token
      workflowToken.put("action_type", "MapReduce");
      workflowToken.put("start_time", Value.of(System.currentTimeMillis()));
    }
  }
}
