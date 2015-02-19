package co.cask.cdap.metrics.data;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DataMigration26 {

  MetricsEntityCodec codec;

  public enum ProgramType {
    FLOWS("f"),
    MAPREDUCE("b"),
    PROCEDURES("p"),
    HANDLERS("h"),
    SERVICES("u"),
    SPARK("s");

    private final String code;
    private static final Map<String, ProgramType> CATEGORY_MAP;

    static {
      CATEGORY_MAP = new HashMap<String, ProgramType>();
      for (ProgramType type : ProgramType.values()) {
        CATEGORY_MAP.put(type.getCode(), type);
      }
    }

    private ProgramType(String code) {
      this.code = code;
    }

    public String getCode() {
      return code;
    }

    public static ProgramType getProgramType(String code) {
      return CATEGORY_MAP.get(code);
    }
  }

  private final List<String> flowTagList = ImmutableList.of(Constants.Metrics.Tag.APP,
                                                            Constants.Metrics.Tag.PROGRAM_TYPE,
                                                            Constants.Metrics.Tag.PROGRAM,
                                                            Constants.Metrics.Tag.FLOWLET,
                                                            Constants.Metrics.Tag.INSTANCE_ID);

  private final List<String> mapTagList = ImmutableList.of(Constants.Metrics.Tag.APP,
                                                            Constants.Metrics.Tag.PROGRAM_TYPE,
                                                            Constants.Metrics.Tag.PROGRAM,
                                                            Constants.Metrics.Tag.MR_TASK_TYPE,
                                                            Constants.Metrics.Tag.INSTANCE_ID);


  private enum MapReduceType {
    MAPPERS("m"),
    REDUCERS("r");

    private final String id;

    private MapReduceType(String id) {
      this.id = id;
    }

    private String getId() {
      return id;
    }
  }

  public DataMigration26(MetricsEntityCodec codec) {
    this.codec = codec;
  }

  // pass rowkey and scope name, since we have removed having separate tables for scope in 2.8
  public MetricValue getMetricValue(byte[] rowKey, String scope) {
    String context = codec.decode(MetricsEntityType.CONTEXT, rowKey);
    String metricName = codec.decode(MetricsEntityType.METRIC, rowKey);
    String runId = codec.decode(MetricsEntityType.RUN, rowKey);
    // todo : see which context's emit tags and add them to tag-value map ?
    String tag = codec.decode(MetricsEntityType.TAG, rowKey);
    Map<String, String> tagValues = getContextTags(context);
    if (runId != null) {
      tagValues.put(Constants.Metrics.Tag.RUN_ID, runId);
    }
    // starting with aggregates table
    MetricValue value = new MetricValue(tagValues, metricName, Long.MAX_VALUE, 0, MetricType.GAUGE);
    return value;
  }

  private Map<String, String> getContextTags(String context) {
    Map<String, String> contextMap = Maps.newHashMap();
    List<String> contextTags = Lists.newArrayList(Splitter.on(".").split(context));
    // begin with application-program metrics , which have program-type at 2nd level of context
    if (contextTags.size() > 1) {

      switch (ProgramType.getProgramType(contextTags.get(1))) {
        case FLOWS:
          return createTagMapping(flowTagList, contextTags, contextMap);
        case MAPREDUCE:
          return createTagMapping(mapTagList, contextTags, contextMap);
      }
    }
    return Maps.newHashMap();
  }

  private Map<String, String> createTagMapping(List<String> keys, List<String> values,
                                               Map<String, String> contextMap) {
    Map<String, String> resultMap = Maps.newHashMap();
    resultMap.put("ns", "default");
    for (int i = 0; i < values.size(); i++) {
      resultMap.put(keys.get(i), values.get(i));
    }
    return resultMap;
  }
}
