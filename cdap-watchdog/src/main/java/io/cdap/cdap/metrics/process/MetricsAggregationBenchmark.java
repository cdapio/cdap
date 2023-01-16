package io.cdap.cdap.metrics.process;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.common.io.StringCachingDecoder;
import io.cdap.cdap.internal.io.ASMDatumWriterFactory;
import io.cdap.cdap.internal.io.ASMFieldAccessorFactory;
import io.cdap.cdap.internal.io.ReflectionDatumReaderFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.metrics.collect.AggregatedMetricsEmitter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.cdap.cdap.api.metrics.MetricType.COUNTER;
import static io.cdap.cdap.api.metrics.MetricType.GAUGE;

public class MetricsAggregationBenchmark {
  private static final Random rand = new Random();
  private static final long NUM_METRICS = 200;
  private static final long NUM_MESSAGES = 1000;

  public static void main(String[] args) throws UnsupportedTypeException, IOException {
    ArrayList<MetricValues> metrics = new ArrayList<>();
    for (int i = 0; i < NUM_MESSAGES; i++) {
      metrics.add(getRandomMetric());
    }

    // One time set up for encoding.
    ByteArrayOutputStream encOS = new ByteArrayOutputStream(1024);
    Encoder enc = new BinaryEncoder(encOS);
    TypeToken<MetricValues> metricValueType = TypeToken.of(MetricValues.class);
    Schema schema = new ReflectionSchemaGenerator().generate(metricValueType.getType());
    DatumWriter<MetricValues> recordWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricValueType, schema);

    // One time set up for decoding.
    HashMap<String, String> cachemap = new HashMap<>();
    PayloadInputStream decOs = new PayloadInputStream();
    StringCachingDecoder decoder = new StringCachingDecoder(new BinaryDecoder(decOs), cachemap);
    DatumReader<MetricValues> reader = new ReflectionDatumReaderFactory().create(metricValueType, schema);

    long start = System.currentTimeMillis();
    ArrayList<byte[]> encoded = new ArrayList<>();
    // System.out.println("Initial metric values: " + mvs);

    for (int i = 0; i < NUM_MESSAGES; i++) {
      // Encode the metric value into a message.
      encOS.reset();
      recordWriter.encode(metrics.get(i), enc);
      byte[] res = encOS.toByteArray();
      encoded.add(res);
      // System.out.println("Encoded metric: " + res);
    }

    long enc_end1 = System.currentTimeMillis();
    System.out.println("Time to initial encode: " + (enc_end1 - start));
    ArrayList<MetricValues> decoded = new ArrayList<>();

    // Now we do the following:
    // decode -> aggregate -> encode

    for (int i = 0; i < NUM_MESSAGES; i++) {
      // Decode the metric value back.
      decOs.reset(encoded.get(i));
      MetricValues got = reader.read(decoder, schema);
      decoded.add(got);
      // System.out.println("Got after decoding: " + got);
    }
    cachemap.clear();
    long dec_end = System.currentTimeMillis();
    System.out.println("Time taken to decode: " + (dec_end - enc_end1));

    // Aggregate the metrics.
    ArrayList<MetricValues> aggregated = aggregateMetricValues(decoded);
    long agg_end = System.currentTimeMillis();
    System.out.println("Time taken to agggregate: " + (agg_end - dec_end));
    System.out.println(String.format("Aggregate from (%s) -> (%s)", decoded.size(), aggregated.size()));

    ArrayList<byte[]> result = new ArrayList<>();
    // Encode back.
    for (int i = 0; i < aggregated.size(); i++) {
      // Encode the metric value into a message.
      encOS.reset();
      recordWriter.encode(aggregated.get(i), enc);
      byte[] res = encOS.toByteArray();
      result.add(res);
      // System.out.println("Encoded metric: " + res);
    }

    long enc_end2 = System.currentTimeMillis();
    System.out.println("Time to encode 2: " + (enc_end2 - agg_end));
    System.out.println("Total time taken: " + (enc_end2 - enc_end1));
  }

  private static MetricValues getRandomMetric() {
    long r = rand.nextInt();
    MetricType type = (r % 2 == 0) ? COUNTER : GAUGE;
    long r2 = r % NUM_METRICS;
    HashMap<String, String> tags = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      tags.put("key-" + i, "val-" + r2);
    }
    ArrayList<MetricValue> metrics = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      metrics.add(new MetricValue("metric-" + r2, type, r2));
    }
    return new MetricValues(tags, r, metrics);
  }

  private static ArrayList<MetricValues> aggregateMetricValues(ArrayList<MetricValues> metrics) {
    ArrayList<MetricValues> result = new ArrayList<>();

    // Group MetricValues with the same tags.
    HashMap<Map<String, String>, ArrayList<MetricValues>> grouped = new HashMap<>();
    for (MetricValues mv : metrics) {
      if (mv.getMetrics().isEmpty()) {
        continue;
      }
      grouped.putIfAbsent(mv.getTags(), new ArrayList<MetricValues>());
      grouped.get(mv.getTags()).add(mv);
    }

    // Combine metrics with the same tags name.
    for (Map.Entry<Map<String, String>, ArrayList<MetricValues>> iter : grouped.entrySet()) {
      ArrayList<MetricValues> sorted = iter.getValue();
      // Sort according to timestamp.
      sorted.sort((MetricValues m1, MetricValues m2) -> {
        return (int) (m1.getTimestamp() - m2.getTimestamp());
      });
      HashMap<String, AggregatedMetricsEmitter> emitters = new HashMap<>();
      ArrayList<MetricValue> aggregated = new ArrayList<>();

      for (MetricValues mvs : sorted) {
        for (MetricValue metric : mvs.getMetrics()) {
          switch (metric.getType()) {
            case COUNTER:
              emitters.computeIfAbsent(metric.getName(), AggregatedMetricsEmitter::new).increment(metric.getValue());
              break;
            case GAUGE:
              emitters.computeIfAbsent(metric.getName(), AggregatedMetricsEmitter::new).gauge(metric.getValue());
              break;
            default:
              // Don't know how to aggregate, so just add it to the result.
              aggregated.add(metric);
          }
        }
      }

      for (AggregatedMetricsEmitter emitter : emitters.values()) {
        aggregated.add(emitter.emit());
      }
      result.add(new MetricValues(iter.getKey(), sorted.get(sorted.size() - 1).getTimestamp(), aggregated));
    }
    return result;
  }

  private static class PayloadInputStream extends ByteArrayInputStream {
    PayloadInputStream() {
      super(Bytes.EMPTY_BYTE_ARRAY);
    }

    void reset(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
      this.count = buf.length;
      this.mark = 0;
    }
  }
}
