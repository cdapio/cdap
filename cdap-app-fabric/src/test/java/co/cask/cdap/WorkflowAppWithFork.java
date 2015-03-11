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
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 *
 */
public class WorkflowAppWithFork extends AbstractApplication {
  @Override
  public void configure() {
    setName("WorkflowAppWithFork");
    setDescription("Workflow App containing fork");
    addMapReduce(new OneWordCountMapReduce());
    addMapReduce(new AnotherWordCountMapReduce());
    addWorkflow(new WorkflowWithFork());
  }

  /**
   *
   */
  public static final class OneWordCountMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("OneWordCountMapReduce");
      setDescription("WordCount job from Hadoop examples");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String inputPath = args.get("oneInputPath");
      String outputPath = args.get("oneOutputPath");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      // No-op
    }
  }

  /**
   *
   */
  public static final class AnotherWordCountMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("AnotherWordCountMapReduce");
      setDescription("WordCount job from Hadoop examples");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String inputPath = args.get("anotherInputPath");
      String outputPath = args.get("anotherOutputPath");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      // No-op
    }
  }

  /**
   *
   */
  public static class WorkflowWithFork extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("WorkflowWithFork");
      setDescription("WorkflowWithFork description");
      fork().addMapReduce("AnotherWordCountMapReduce").also().addMapReduce("OneWordCountMapReduce").join();
    }
  }
}
