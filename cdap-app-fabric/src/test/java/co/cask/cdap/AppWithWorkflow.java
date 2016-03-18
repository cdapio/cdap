/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * App with workflow.
 */
public class AppWithWorkflow extends AbstractApplication {
  public static final String NAME = "AppWithWorkflow";

  @Override
  public void configure() {
    try {
      setName(NAME);
      setDescription("Sample application");
      addStream(new Stream("stream"));
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
    public static final String TABLE_NAME = "MyTable";
    public static final String FILE_NAME = "MyFile";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("SampleWorkflow description");
      createLocalDataset(TABLE_NAME, KeyValueTable.class, DatasetProperties.builder().add("foo", "bar").build());
      createLocalDataset(FILE_NAME, FileSet.class, DatasetProperties.builder().add("anotherFoo", "anotherBar").build());
      addAction(new DummyAction(FIRST_ACTION));
      addAction(new DummyAction(SECOND_ACTION));
      addMapReduce(WordCountMapReduce.class.getSimpleName());
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractWorkflowAction {
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
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      WorkflowToken workflowToken = context.getToken();
      workflowToken.put(TOKEN_KEY, TOKEN_VALUE);
    }

    @Override
    public void run() {
      LOG.info("Ran dummy action");
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
    @SuppressWarnings("ConstantConditions")
    public void beforeSubmit(MapReduceContext context) throws Exception {
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

