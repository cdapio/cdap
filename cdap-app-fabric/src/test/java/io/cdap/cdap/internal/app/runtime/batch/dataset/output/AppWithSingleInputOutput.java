/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * App used to test whether map reduce task context has correct config values.
 */
public class AppWithSingleInputOutput extends AbstractApplication {
  static final String ADDITIONAL_CONFIG = "additionalConfig";
  static final String SINK_CONFIG = "sinkConfig";
  static final String SOURCE_CONFIG = "sinkConfig";


  @Override
  public void configure() {
    setName("AppWithSingleInputOutput");
    setDescription("Application with MapReduce job");
    addMapReduce(new SimpleMapReduce());
  }

  /**
   * Simple map-only MR.
   */
  public static class SimpleMapReduce extends AbstractMapReduce {
    @Override
    protected void configure() {
      setName("SimpleMapReduce");
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();

      Map<String, String> sourceConf = new HashMap<>();
      sourceConf.put(ADDITIONAL_CONFIG, SOURCE_CONFIG);

      context.addInput(Input.of("input", new InputFormatProvider() {
        @Override
        public String getInputFormatClassName() {
          return ConfigVerifyingInputFormat.class.getName();
        }

        @Override
        public Map<String, String> getInputFormatConfiguration() {
          return sourceConf;
        }
      }), SimpleMapper.class);

      Map<String, String> sinkConf = new HashMap<>();
      sinkConf.put(ADDITIONAL_CONFIG, SINK_CONFIG);
      context.addOutput(Output.of("test", new OutputFormatProvider() {
        @Override
        public String getOutputFormatClassName() {
          return ConfigVerifyingOutputFormat.class.getName();
        }

        @Override
        public Map<String, String> getOutputFormatConfiguration() {
          return sinkConf;
        }
      }));

      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      job.setNumReduceTasks(0);
    }
  }

  public static class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<LongWritable, Text>> {

    @Override
    public void initialize(MapReduceTaskContext<LongWritable, Text> context) throws Exception {
      // no-op
    }

    @Override
    public void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(1L), data);
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}
