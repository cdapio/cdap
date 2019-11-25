/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.spark.sparkjoin;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A spark joiner that is able to (inner) join inputs by key, like a genuine Joiner plugin (and more so)
 */
@Name("MockSparkJoiner")
public class MockSparkJoiner extends SparkJoiner<StructuredRecord> {

    public static final PluginClass PLUGIN_CLASS = getPluginClass();

    private Conf conf;

    public MockSparkJoiner(Conf conf) {
        this.conf = conf;
    }

    private  class JoinOn implements PairFunction<StructuredRecord,String,StructuredRecord> {
        private final String keyField;

        public JoinOn(String keyField) {
            this.keyField = keyField;
        }

        @Override
        public Tuple2<String, StructuredRecord> call(StructuredRecord structuredRecord) throws Exception {
            return new Tuple2<>(structuredRecord.get(keyField), structuredRecord);
        }
    }

    private  class MergeRDDs
            implements Function<
            Tuple2<String, Tuple2<StructuredRecord, StructuredRecord>>, StructuredRecord> {



        @Override
        public StructuredRecord call(Tuple2<String, Tuple2<StructuredRecord, StructuredRecord>> joinedInputs) throws Exception {
            StructuredRecord.Builder builder = StructuredRecord.builder(schema);

            mapping.forEach( (k,v)->{
                Tuple2<StructuredRecord, StructuredRecord> tuple2 = joinedInputs._2;
                builder.set(k, v._1.equals(conf.source1)? tuple2._1.get(v._2): tuple2._2.get(v._2));
            });

            return builder.build();
        }
    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        super.initialize(context);


        schema = Schema.parseJson(conf.outputSchema);

        String[] kvs = conf.mapping.split(";");

        mapping = new HashMap<>();
        for (String kv : kvs) {
            String[] parts = kv.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Expected format [out field]:[input source]:[ input field]. Got "+kv);
            }
            if (!conf.source1.equals(parts[1])&&!conf.source2.equals(parts[1])) {
                throw new IllegalArgumentException("Unknown source: "+parts[1]);
            }
            mapping.put(parts[0], new Tuple2<>(parts[1], parts[2]));
        }

    }

    private Map<String, Tuple2<String,String>> mapping;
    private Schema schema;

    @Override
    public JavaRDD<StructuredRecord> join(SparkExecutionPluginContext context, Map<String, JavaRDD<?>> inputs) throws Exception {

        if (inputs.size() !=2) {
            throw new IllegalArgumentException("this plugin can join exaclty two inputs. Found "+ inputs.size());
        }

        JavaRDD<StructuredRecord> rdd1 = (JavaRDD<StructuredRecord>) inputs.get(conf.source1);
        if (rdd1 == null) {
            throw new IllegalArgumentException("No source1 found with name "+conf.source1);
        }
        JavaRDD<StructuredRecord> rdd2 = (JavaRDD<StructuredRecord>) inputs.get(conf.source2);
        if (rdd2 == null) {
            throw new IllegalArgumentException("No source2 found with name "+conf.source2);
        }



        return rdd1.mapToPair(new JoinOn(conf.keyField1))
                .join(rdd2.mapToPair(new JoinOn(conf.keyField2)))
                .map(new MergeRDDs());

    }

    public static class Conf extends PluginConfig {
        @Name("source1")
        public String source1;

        @Name("keyField1")
        public String keyField1;

        @Name("source2")
        public String source2;

        @Name("keyField2")
        public String keyField2;

        @Name("outputSchema")
        public String outputSchema;

        @Name("mapping")
        public String mapping;

    }

    public static ETLPlugin getPlugin(String source1, String keyField1, String source2, String keyField2,
                                                   Schema outputSchema, String mapping) {

        Map<String,String> properties = new HashMap<>();

        properties.put("source1", source1);
        properties.put("keyField1", keyField1);
        properties.put("source2", source2);
        properties.put("keyField2", keyField2);
        properties.put("outputSchema", outputSchema.toString());
        properties.put("mapping", mapping);

        return new ETLPlugin("MockSparkJoiner", SparkJoiner.PLUGIN_TYPE, properties, null);

    }

    private static PluginClass getPluginClass() {
        Map<String, PluginPropertyField> properties = new HashMap<>();
        properties.put("source1", new PluginPropertyField("source1", "", "string", true, true));
        properties.put("keyField1", new PluginPropertyField("keyField1", "", "string", true, true));
        properties.put("source2", new PluginPropertyField("source2", "", "string", true, true));
        properties.put("keyField2", new PluginPropertyField("keyField2", "", "string", true, true));
        properties.put("outputSchema", new PluginPropertyField("outputSchema", "", "string", true, true));
        properties.put("mapping", new PluginPropertyField("mapping", "", "string", true, true));
        return new PluginClass(SparkJoiner.PLUGIN_TYPE, "MockSparkJoiner", "", MockSparkJoiner.class.getName(),
                "conf", properties);
    }

}
