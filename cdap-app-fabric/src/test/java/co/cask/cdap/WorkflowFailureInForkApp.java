/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * App to test failure in the Workflow fork.
 */
public class WorkflowFailureInForkApp extends AbstractApplication {
  public static final String NAME = "WorkflowFailureInForkApp";
  public static final String FIRST_MAPREDUCE_NAME = "FirstMapReduce";
  public static final String SECOND_MAPREDUCE_NAME = "SecondMapReduce";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application to test the failure in the Workflow fork.");
    addMapReduce(new WordCountMapReduce(FIRST_MAPREDUCE_NAME));
    addMapReduce(new WordCountMapReduce(SECOND_MAPREDUCE_NAME));
    addWorkflow(new WorkflowWithFailureInFork());
  }

  /**
   * Workflow with failure in the fork.
   */
  public class WorkflowWithFailureInFork extends AbstractWorkflow {
    public static final String NAME = "WorkflowWithFailureInFork";

    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Workflow with failure in the fork.");
      fork()
        .addMapReduce(FIRST_MAPREDUCE_NAME)
      .also()
        .addMapReduce(SECOND_MAPREDUCE_NAME)
      .join();
    }
  }

  /**
   * WordCount MapReduce job
   */
  public static final class WordCountMapReduce extends AbstractMapReduce {
    private final String name;

    public WordCountMapReduce(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
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
      if (args.containsKey("throw.exception")) {
        File file = new File(args.get("sync.file"));
        while (!file.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }

        file = new File(args.get("wait.file"));
        //noinspection ResultOfMethodCallIgnored
        file.createNewFile();
        throw new RuntimeException("Exception in beforeSubmit()");
      }
      File file = new File(args.get("sync.file"));
      //noinspection ResultOfMethodCallIgnored
      file.createNewFile();

      // Wait till the SecondMapReduce program is ready to throw an exception
      file = new File(args.get("wait.file"));
      while (!file.exists()) {
        TimeUnit.MILLISECONDS.sleep(50);
      }
    }
  }
}
