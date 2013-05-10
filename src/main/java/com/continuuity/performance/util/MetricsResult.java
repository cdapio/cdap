package com.continuuity.performance.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for Mensa metrics.
 */
final class MetricsResult {
  private final List<Metric> metrics;
  private final Map<String, Metric> metricsMap;

  public MetricsResult(List<Metric> metrics) {
    this.metrics = metrics;
    this.metricsMap = new HashMap<String, Metric>(metrics.size());
    for (Metric m : metrics) {
      this.metricsMap.put(m.metric, m);
    }
  }

  public List<Metric> getMetrics() {
    return metrics;
  }

  public Metric getMetric(int index) {
    return metrics.get(index);
  }

  static final class Metric {
    private final String metric;
    private final Map<String, String> tags;
    private final List<DataPoint> data;

    public Metric(final String metric, final Map<String, String> tags, final List<DataPoint> datapoints) {
      this.metric = metric;
      this.tags = tags;
      this.data = datapoints;
    }

    public void dump() {
      for (DataPoint dp : data) {
        StringBuilder sb = new StringBuilder();
        sb.append(metric);
        sb.append(" ");
        sb.append(dp.ts);
        sb.append(" ");
        sb.append(dp.val);
        for (Map.Entry<String, String> tag : tags.entrySet()) {
          sb.append(" ");
          sb.append(tag.getKey());
          sb.append("=");
          sb.append(tag.getValue());
        }
        System.out.println(sb.toString());
      }

    }

    public int getNumDataPoints() {
      return data.size();
    }

    public double sum() {
      double sum = 0;
      for (DataPoint dp : data) {
        sum += dp.val;
      }
      return sum;
    }

    public double sum(int x) {
      if (data.size() == 0) {
        return 0;
      }
      int num = x;
      if (num > data.size()) {
        num = data.size();
      }
      double sum = 0;
      for (int i = data.size() - num; i < data.size(); i++) {
        sum += data.get(i).val;
      }
      return sum;
    }

    public double avg() {
      if (data.size() == 0) {
        return 0;
      }
      return sum() / data.size();
    }

    public double avg(int x) {
      if (data.size() == 0) {
        return 0;
      }
      int num = x;
      if (num > data.size()) {
        num = data.size();
      }
      return sum(x) / num;
    }
    static final class DataPoint {
      private final long ts;
      private final double val;

      public DataPoint(final long ts, final double val) {
        this.ts = ts;
        this.val = val;
      }
    }

    static final class DataPointDeserializer implements JsonDeserializer<DataPoint> {
      @Override
      public DataPoint deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
        JsonArray a = json.getAsJsonArray();
        long ts = a.get(0).getAsLong();
        double val = a.get(1).getAsDouble();
        return new DataPoint(ts, val);
      }
    }
  }
}
