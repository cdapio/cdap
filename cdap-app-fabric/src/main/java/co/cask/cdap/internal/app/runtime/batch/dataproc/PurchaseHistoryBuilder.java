/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class PurchaseHistoryBuilder extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
        MapReduceContext context = getContext();
        Job job = context.getHadoopJob();
        //job.setMapperClass(PurchaseMapper.class);
        job.setReducerClass(PerUserReducer.class);


        context.addInput(Input.of("name", new InputFormatProvider() {
            @Override
            public String getInputFormatClassName() {
                return TextInputFormat.class.getName();
            }

            @Override
            public Map<String, String> getInputFormatConfiguration() {
                return ImmutableMap.of(TextInputFormat.INPUT_DIR, "/tmp/input");
            }
        }), PurchaseMapper.class);
        context.addOutput(Output.of("name2", new OutputFormatProvider() {
            @Override
            public String getOutputFormatClassName() {
                return TextOutputFormat.class.getName();
            }

            @Override
            public Map<String, String> getOutputFormatConfiguration() {
                return ImmutableMap.of(TextOutputFormat.OUTDIR, "/tmp/output");
            }
        }));
        //FileInputFormat.addInputPath(job, new Path("/tmp/input"));
        //FileOutputFormat.setOutputPath(job, new Path("/tmp/output"));
    }

    /**
     * Mapper class to emit user and corresponding purchase information
     */
    public static class PurchaseMapper extends Mapper<LongWritable, Text, Text, Text> {

        private JsonParser jsonParser = new JsonParser();

        @Override
        public void map(LongWritable key, Text purchase, Context context) throws IOException, InterruptedException {
            String user = purchase.toString().split(",")[0];
            context.write(new Text(user), purchase);
        }
    }

    /**
     * Reducer class to aggregate all purchases per user
     */
    public static class PerUserReducer extends Reducer<Text, Text, String, IntWritable> {

        //@UseDataSet("frequentCustomers")
        //private KeyValueTable frequentCustomers;

        //private Metrics reduceMetrics;

        //private static final Logger LOG = LoggerFactory.getLogger(PerUserReducer.class);

        @Override
        public void reduce(Text customer, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int spend = 0;
            for (Text value : values) {
                spend += getSpend(value.toString());
            }
            context.write(customer.toString(), new IntWritable(spend));
        }

        private int getSpend(String purchase) {
            String[] split = purchase.split(",");
            // spend = cost * quantity
            return Integer.parseInt(split[2]) * Integer.parseInt(split[3]);
        }
    }
}
