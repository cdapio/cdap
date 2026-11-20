/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.SparkExecutionContext;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.runtime.spark.serializer.SchemaSerializer;
import io.cdap.cdap.app.runtime.spark.serializer.StructuredRecordSerializer;
import io.cdap.cdap.app.runtime.spark.serializer.UnmodifiableListSerializer;
import io.cdap.cdap.app.runtime.spark.serializer.UnmodifiableMapSerializer;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Unit tests for {@link OpaqueDatasetCollection} covering Spark 3.5.x {@code encoderFor},
 * Spark 3.3.2 reflective {@code RowEncoder.apply} fallback, and {@link DataframeCollection}
 * conversion across primitive, nullable, collection, and nested schemas.
 */
public class OpaqueDatasetCollectionTest {

  private static final AtomicInteger FALLBACK_APPLY_CALLS = new AtomicInteger();
  private static final AtomicBoolean FAIL_FALLBACK_APPLY = new AtomicBoolean();

  private static final Schema NESTED_SCHEMA = Schema.recordOf(
      "nestedRec",
      Schema.Field.of("innerStr", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("innerNums", Schema.arrayOf(Schema.of(Schema.Type.INT)))
  );

  private static final Schema COMPLEX_SCHEMA = Schema.recordOf(
      "complexRec",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("flag", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("count", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ratio", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("payload", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("optionalNote", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("tags", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("metrics",
          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
      Schema.Field.of("nested", NESTED_SCHEMA),
      Schema.Field.of("optionalNested", Schema.nullableOf(NESTED_SCHEMA))
  );

  private static SparkSession sparkSession;
  private static JavaSparkContext jsc;
  private static SQLContext sqlContext;

  /**
   * Initializes the shared local {@link SparkSession} for unit tests.
   */
  @BeforeClass
  public static void setUpClass() {
    sparkSession = SparkSession.builder()
        .master("local[2]")
        .appName("OpaqueDatasetCollectionTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.kryo.registrator", TestKryoRegistrator.class.getName())
        .getOrCreate();
    jsc = new JavaSparkContext(sparkSession.sparkContext());
    sqlContext = sparkSession.sqlContext();
  }

  /**
   * Stops the shared local {@link SparkSession}.
   */
  @AfterClass
  public static void tearDownClass() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
  }

  /**
   * Bridge invoked by the synthetic Spark-3.3.2-shaped {@code RowEncoder.apply(StructType)} class.
   */
  public static ExpressionEncoder<Row> invokeSpark33FallbackBridge(StructType sparkSchema) {
    FALLBACK_APPLY_CALLS.incrementAndGet();
    if (FAIL_FALLBACK_APPLY.get()) {
      throw new IllegalArgumentException("Simulated Spark 3.3 RowEncoder.apply failure");
    }
    return OpaqueDatasetCollection.getRowEncoder(sparkSchema);
  }

  @Test
  public void testGetRowEncoderComplexSchemaAndCacheReuse() {
    StructType sparkSchema1 = DataFrames.toDataType(COMPLEX_SCHEMA);
    StructType sparkSchema2 = DataFrames.toDataType(COMPLEX_SCHEMA);
    Assert.assertNotSame(sparkSchema1, sparkSchema2);
    Assert.assertEquals(sparkSchema1, sparkSchema2);

    ExpressionEncoder<Row> encoder1 = OpaqueDatasetCollection.getRowEncoder(sparkSchema1);
    Assert.assertNotNull(encoder1);
    Assert.assertEquals(sparkSchema1, encoder1.schema());
    Assert.assertEquals(Row.class, encoder1.clsTag().runtimeClass());

    ExpressionEncoder<Row> encoder2 = OpaqueDatasetCollection.getRowEncoder(sparkSchema2);
    Assert.assertSame(encoder1, encoder2);
  }

  @Test
  public void testFromDatasetRejectsRowDataset() {
    StructType sparkSchema = DataFrames.toDataType(COMPLEX_SCHEMA);
    Dataset<Row> rowDataset = sqlContext.createDataFrame(Collections.emptyList(), sparkSchema);
    try {
      OpaqueDatasetCollection.fromDataset(
          rowDataset, new TestJavaSparkExecutionContext(Collections.emptyMap()),
          jsc, sqlContext, null, null, FunctionCache.Factory.newInstance(false));
      Assert.fail("Expected IllegalArgumentException when passing Dataset<Row> to fromDataset");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("Opaque collection received dataset of Row"));
    }
  }

  @Test
  public void testFromRddWithKryoToDataframeCollection() {
    StructuredRecord rec1 = buildFullRecord();
    StructuredRecord rec2 = buildNullsRecord();

    JavaRDD<StructuredRecord> rdd = jsc.parallelize(Arrays.asList(rec2, rec1), 2);
    JavaSparkExecutionContext kryoSec = new TestJavaSparkExecutionContext(Collections.emptyMap());

    OpaqueDatasetCollection<StructuredRecord> opaque = OpaqueDatasetCollection.fromRdd(
        rdd, kryoSec, jsc, sqlContext, null, null, FunctionCache.Factory.newInstance(false));
    DataframeCollection dfCollection = opaque.toDataframeCollection(COMPLEX_SCHEMA);

    Dataset<Row> df = dfCollection.getDataframe();
    Assert.assertEquals(DataFrames.toDataType(COMPLEX_SCHEMA), df.schema());

    List<Row> rows = df.orderBy("id").collectAsList();
    Assert.assertEquals(2, rows.size());
    assertMatchesFullRow(rows.get(0));
    assertMatchesNullsRow(rows.get(1));

    List<StructuredRecord> roundTripped = dfCollection.getJavaRDD().collect();
    Assert.assertEquals(2, roundTripped.size());
    Map<Integer, StructuredRecord> byId = new HashMap<>();
    for (StructuredRecord record : roundTripped) {
      byId.put(record.get("id"), record);
    }
    Assert.assertEquals("alpha", byId.get(1).get("name"));
    Assert.assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), byId.get(1).get("payload"));
    Assert.assertEquals("inner-val",
        byId.get(1).<StructuredRecord>get("nested").get("innerStr"));
    Assert.assertNull(byId.get(2).get("optionalNote"));
    Assert.assertNull(byId.get(2).get("optionalNested"));
  }

  @Test
  public void testFromRddWithJavaSerializationAndFromDataset() {
    StructuredRecord rec1 = buildFullRecord();
    StructuredRecord rec2 = buildNullsRecord();

    JavaRDD<StructuredRecord> rdd = jsc.parallelize(Arrays.asList(rec1, rec2), 2);
    JavaSparkExecutionContext javaSec = new TestJavaSparkExecutionContext(
        Collections.singletonMap(Constants.DATASET_KRYO_ENABLED, Boolean.FALSE.toString()));

    OpaqueDatasetCollection<StructuredRecord> fromRdd = OpaqueDatasetCollection.fromRdd(
        rdd, javaSec, jsc, sqlContext, null, null, FunctionCache.Factory.newInstance(false));
    OpaqueDatasetCollection<StructuredRecord> fromDataset = OpaqueDatasetCollection.fromDataset(
        fromRdd.getDataset(), javaSec, jsc, sqlContext, null, null,
        FunctionCache.Factory.newInstance(false));

    List<Row> filteredRows = fromDataset.toDataframeCollection(COMPLEX_SCHEMA)
        .getDataframe()
        .filter("id = 1")
        .collectAsList();
    Assert.assertEquals(1, filteredRows.size());
    assertMatchesFullRow(filteredRows.get(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSpark33ReflectiveApplyFallbackAndErrorReporting() throws Exception {
    FALLBACK_APPLY_CALLS.set(0);
    FAIL_FALLBACK_APPLY.set(false);

    StructType fallbackSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("k", DataTypes.StringType, false),
        DataTypes.createStructField("v", DataTypes.IntegerType, true)
    ));

    // 1. Verify Spark 3.3.2 fallback (no encoderFor, only RowEncoder.apply(StructType)) + caching
    ClassLoader spark33Loader = new SyntheticRowEncoderClassLoader(true);
    Class<?> isolatedOpaque = spark33Loader.loadClass(OpaqueDatasetCollection.class.getName());
    Method getRowEncoder = isolatedOpaque.getMethod("getRowEncoder", StructType.class);

    ExpressionEncoder<Row> fallbackEncoder =
        (ExpressionEncoder<Row>) getRowEncoder.invoke(null, fallbackSchema);
    Assert.assertNotNull(fallbackEncoder);
    Assert.assertEquals(1, FALLBACK_APPLY_CALLS.get());

    // Second invocation on equal StructType hits isolated ENCODER_CACHE without re-invoking apply
    ExpressionEncoder<Row> cachedFallbackEncoder =
        (ExpressionEncoder<Row>) getRowEncoder.invoke(null, fallbackSchema);
    Assert.assertSame(fallbackEncoder, cachedFallbackEncoder);
    Assert.assertEquals(1, FALLBACK_APPLY_CALLS.get());

    // Materialize a real Spark Dataset<Row> using the encoder returned by the fallback branch
    List<Row> materialized = sparkSession.createDataset(
        Arrays.asList(RowFactory.create("one", 1), RowFactory.create("two", null)),
        fallbackEncoder).collectAsList();
    Assert.assertEquals(2, materialized.size());
    Assert.assertEquals("one", materialized.get(0).getString(0));
    Assert.assertEquals(1, materialized.get(0).getInt(1));
    Assert.assertTrue(materialized.get(1).isNullAt(1));

    // 2. Verify informative error reporting when both encoderFor and apply fail
    FAIL_FALLBACK_APPLY.set(true);
    try {
      ClassLoader failingApplyLoader = new SyntheticRowEncoderClassLoader(true);
      Class<?> failingOpaque =
          failingApplyLoader.loadClass(OpaqueDatasetCollection.class.getName());
      Method failingGetEncoder = failingOpaque.getMethod("getRowEncoder", StructType.class);
      try {
        failingGetEncoder.invoke(null, fallbackSchema);
        Assert.fail("Expected UncheckedExecutionException when both encoder paths fail");
      } catch (InvocationTargetException expected) {
        Throwable unwrapped = expected.getCause();
        Assert.assertTrue(unwrapped instanceof UncheckedExecutionException);
        String msg = unwrapped.getCause().getMessage();
        Assert.assertTrue(msg.contains("Failed to load a suitable Encoder dynamically."));
        Assert.assertTrue(msg.contains("encoderFor"));
        Assert.assertTrue(msg.contains("Simulated Spark 3.3 RowEncoder.apply failure"));
      }
    } finally {
      FAIL_FALLBACK_APPLY.set(false);
    }

    // 3. Verify informative error reporting when neither encoderFor nor apply method exists
    ClassLoader emptyRowEncoderLoader = new SyntheticRowEncoderClassLoader(false);
    Class<?> emptyOpaque =
        emptyRowEncoderLoader.loadClass(OpaqueDatasetCollection.class.getName());
    Method emptyGetEncoder = emptyOpaque.getMethod("getRowEncoder", StructType.class);
    try {
      emptyGetEncoder.invoke(null, fallbackSchema);
      Assert.fail("Expected UncheckedExecutionException when RowEncoder has neither method");
    } catch (InvocationTargetException expected) {
      Throwable unwrapped = expected.getCause();
      Assert.assertTrue(unwrapped instanceof UncheckedExecutionException);
      String msg = unwrapped.getCause().getMessage();
      Assert.assertTrue(msg.contains("encoderFor"));
      Assert.assertTrue(msg.contains("apply"));
    }
  }

  @Test
  public void testBytecodeUsesReflectionWithoutDirectRowEncoderCalls() throws IOException {
    byte[] classBytes = readClassBytes(OpaqueDatasetCollection.class.getName());
    List<String> directRowEncoderCalls = new ArrayList<>();
    Set<String> stringConstants = new HashSet<>();
    AtomicBoolean hasClassGetMethodCall = new AtomicBoolean(false);

    ClassReader reader = new ClassReader(classBytes);
    reader.accept(new ClassVisitor(Opcodes.ASM7) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String descriptor,
          String signature, String[] exceptions) {
        return new MethodVisitor(Opcodes.ASM7) {
          @Override
          public void visitMethodInsn(int opcode, String owner, String methodName,
              String methodDesc, boolean isInterface) {
            if ("org/apache/spark/sql/catalyst/encoders/RowEncoder".equals(owner)) {
              directRowEncoderCalls.add(methodName + methodDesc);
            }
            if ("java/lang/Class".equals(owner) && "getMethod".equals(methodName)) {
              hasClassGetMethodCall.set(true);
            }
          }

          @Override
          public void visitLdcInsn(Object value) {
            if (value instanceof String) {
              stringConstants.add((String) value);
            }
          }
        };
      }
    }, 0);

    Assert.assertEquals("OpaqueDatasetCollection must not link directly to RowEncoder methods",
        Collections.emptyList(), directRowEncoderCalls);
    Assert.assertTrue(hasClassGetMethodCall.get());
    Assert.assertTrue(stringConstants.contains("encoderFor"));
    Assert.assertTrue(stringConstants.contains("apply"));
    Assert.assertTrue(
        stringConstants.contains("org.apache.spark.sql.catalyst.encoders.AgnosticEncoder"));
  }

  private static StructuredRecord buildFullRecord() {
    StructuredRecord nested = StructuredRecord.builder(NESTED_SCHEMA)
        .set("innerStr", "inner-val")
        .set("innerNums", Arrays.asList(10, 20))
        .build();
    StructuredRecord optNested = StructuredRecord.builder(NESTED_SCHEMA)
        .set("innerStr", "opt-inner")
        .set("innerNums", Collections.singletonList(99))
        .build();
    Map<String, Long> metrics = new LinkedHashMap<>();
    metrics.put("ok", 5L);
    metrics.put("missing", null);

    return StructuredRecord.builder(COMPLEX_SCHEMA)
        .set("id", 1)
        .set("flag", true)
        .set("count", 1000L)
        .set("ratio", 1.25f)
        .set("score", 99.5d)
        .set("name", "alpha")
        .set("payload", "bytes".getBytes(StandardCharsets.UTF_8))
        .set("optionalNote", "present")
        .set("tags", Arrays.asList("t1", null, "t3"))
        .set("metrics", metrics)
        .set("nested", nested)
        .set("optionalNested", optNested)
        .build();
  }

  private static StructuredRecord buildNullsRecord() {
    StructuredRecord nested = StructuredRecord.builder(NESTED_SCHEMA)
        .set("innerStr", "minimal")
        .set("innerNums", Collections.emptyList())
        .build();
    return StructuredRecord.builder(COMPLEX_SCHEMA)
        .set("id", 2)
        .set("flag", false)
        .set("count", 0L)
        .set("ratio", 0.0f)
        .set("score", 0.0d)
        .set("name", "beta")
        .set("payload", new byte[0])
        .set("optionalNote", null)
        .set("tags", Collections.emptyList())
        .set("metrics", Collections.emptyMap())
        .set("nested", nested)
        .set("optionalNested", null)
        .build();
  }

  private static void assertMatchesFullRow(Row row) {
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertTrue(row.getBoolean(1));
    Assert.assertEquals(1000L, row.getLong(2));
    Assert.assertEquals(1.25f, row.getFloat(3), 0.0001f);
    Assert.assertEquals(99.5d, row.getDouble(4), 0.0001d);
    Assert.assertEquals("alpha", row.getString(5));
    Assert.assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), (byte[]) row.get(6));
    Assert.assertEquals("present", row.getString(7));
    Assert.assertEquals(Arrays.asList("t1", null, "t3"), row.getList(8));
    Map<String, Long> expectedMetrics = new LinkedHashMap<>();
    expectedMetrics.put("ok", 5L);
    expectedMetrics.put("missing", null);
    Assert.assertEquals(expectedMetrics, row.getJavaMap(9));

    Row nestedRow = row.getStruct(10);
    Assert.assertEquals("inner-val", nestedRow.getString(0));
    Assert.assertEquals(Arrays.asList(10, 20), nestedRow.getList(1));

    Row optNestedRow = row.getStruct(11);
    Assert.assertEquals("opt-inner", optNestedRow.getString(0));
    Assert.assertEquals(Collections.singletonList(99), optNestedRow.getList(1));
  }

  private static void assertMatchesNullsRow(Row row) {
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertFalse(row.getBoolean(1));
    Assert.assertTrue(row.isNullAt(7));
    Assert.assertTrue(row.getList(8).isEmpty());
    Assert.assertTrue(row.getJavaMap(9).isEmpty());
    Assert.assertEquals("minimal", row.getStruct(10).getString(0));
    Assert.assertTrue(row.isNullAt(11));
  }

  private static byte[] readClassBytes(String className) throws IOException {
    String resource = className.replace('.', '/') + ".class";
    try (InputStream in = OpaqueDatasetCollectionTest.class.getClassLoader()
        .getResourceAsStream(resource)) {
      Assert.assertNotNull("Missing class resource: " + resource, in);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static byte[] generateSpark33RowEncoderBytes(boolean withApplyMethod) {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
        "org/apache/spark/sql/catalyst/encoders/RowEncoder", null, "java/lang/Object", null);
    if (withApplyMethod) {
      String desc = "(Lorg/apache/spark/sql/types/StructType;)"
          + "Lorg/apache/spark/sql/catalyst/encoders/ExpressionEncoder;";
      MethodVisitor mv = cw.visitMethod(
          Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "apply", desc, null, null);
      mv.visitCode();
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitMethodInsn(Opcodes.INVOKESTATIC,
          OpaqueDatasetCollectionTest.class.getName().replace('.', '/'),
          "invokeSpark33FallbackBridge", desc, false);
      mv.visitInsn(Opcodes.ARETURN);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static final class SyntheticRowEncoderClassLoader extends ClassLoader {
    private static final String ROW_ENCODER_NAME =
        "org.apache.spark.sql.catalyst.encoders.RowEncoder";
    private final boolean withApplyMethod;

    SyntheticRowEncoderClassLoader(boolean withApplyMethod) {
      super(OpaqueDatasetCollectionTest.class.getClassLoader());
      this.withApplyMethod = withApplyMethod;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          if (ROW_ENCODER_NAME.equals(name)) {
            byte[] bytes = generateSpark33RowEncoderBytes(withApplyMethod);
            loaded = defineClass(name, bytes, 0, bytes.length);
          } else if (name.equals(OpaqueDatasetCollection.class.getName())
              || name.startsWith(OpaqueDatasetCollection.class.getName() + "$")) {
            try {
              byte[] bytes = readClassBytes(name);
              loaded = defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
              throw new ClassNotFoundException("Failed to load " + name, e);
            }
          } else {
            loaded = super.loadClass(name, false);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  /**
   * Mirrors CDAP's {@code SparkClassRewriter.rewriteKryo} registrations for standalone Spark tests.
   */
  public static final class TestKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
      kryo.addDefaultSerializer(Schema.class, SchemaSerializer.class);
      kryo.addDefaultSerializer(StructuredRecord.class, StructuredRecordSerializer.class);
      kryo.addDefaultSerializer(
          Collections.unmodifiableList(new ArrayList<>()).getClass(),
          UnmodifiableListSerializer.class);
      kryo.addDefaultSerializer(
          Collections.unmodifiableMap(new HashMap<>()).getClass(),
          UnmodifiableMapSerializer.class);
    }
  }

  private static final class TestJavaSparkExecutionContext extends JavaSparkExecutionContext {
    private final Map<String, String> runtimeArgs;

    TestJavaSparkExecutionContext(Map<String, String> runtimeArgs) {
      this.runtimeArgs = runtimeArgs;
    }

    @Override
    public Map<String, String> getRuntimeArguments() {
      return runtimeArgs;
    }

    @Override
    public SparkSpecification getSpecification() {
      return null;
    }

    @Override
    public long getLogicalStartTime() {
      return 0;
    }

    @Override
    public long getTerminationTime() {
      return 0;
    }

    @Override
    public ServiceDiscoverer getServiceDiscoverer() {
      return null;
    }

    @Override
    public Metrics getMetrics() {
      return null;
    }

    @Override
    public PluginContext getPluginContext() {
      return null;
    }

    @Override
    public SecureStore getSecureStore() {
      return null;
    }

    @Override
    public MessagingContext getMessagingContext() {
      return null;
    }

    @Override
    public TaskLocalizationContext getLocalizationContext() {
      return null;
    }

    @Override
    public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments,
        @Nullable Iterable<? extends Split> splits) {
      return null;
    }

    @Override
    public <K, V> JavaPairRDD<K, V> fromDataset(String namespace, String datasetName,
        Map<String, String> arguments, @Nullable Iterable<? extends Split> splits) {
      return null;
    }

    @Override
    public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName,
        Map<String, String> arguments) {
    }

    @Override
    public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String namespace, String datasetName,
        Map<String, String> arguments) {
    }

    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {
    }

    @Override
    public void execute(int timeoutInSeconds, TxRunnable runnable)
        throws TransactionFailureException {
    }

    @Override
    public SparkInterpreter createInterpreter() throws IOException {
      return null;
    }

    @Override
    public SparkExecutionContext getSparkExecutionContext() {
      return null;
    }

    @Override
    public ApplicationSpecification getApplicationSpecification() {
      return null;
    }

    @Override
    public String getClusterName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return "default";
    }

    @Override
    public RunId getRunId() {
      return null;
    }

    @Override
    public Admin getAdmin() {
      return null;
    }

    @Override
    public DataTracer getDataTracer(String dataTracerName) {
      return null;
    }

    @Nullable
    @Override
    public TriggeringScheduleInfo getTriggeringScheduleInfo() {
      return null;
    }

    @Override
    public void record(Collection<? extends Operation> operations) {
    }

    @Override
    public void flushLineage() {
    }

    @Override
    public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity)
        throws MetadataException {
      return Collections.emptyMap();
    }

    @Override
    public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity)
        throws MetadataException {
      return null;
    }

    @Override
    public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
    }

    @Override
    public void addTags(MetadataEntity metadataEntity, String... tags) {
    }

    @Override
    public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {
    }

    @Override
    public void removeMetadata(MetadataEntity metadataEntity) {
    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity) {
    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity, String... keys) {
    }

    @Override
    public void removeTags(MetadataEntity metadataEntity) {
    }

    @Override
    public void removeTags(MetadataEntity metadataEntity, String... tags) {
    }

    @Override
    public List<SecureStoreMetadata> list(String namespace) {
      return Collections.emptyList();
    }

    @Override
    public SecureStoreData get(String namespace, String name) {
      return null;
    }

    @Nullable
    @Override
    public WorkflowToken getWorkflowToken() {
      return null;
    }

    @Nullable
    @Override
    public WorkflowInfo getWorkflowInfo() {
      return null;
    }
  }
}
