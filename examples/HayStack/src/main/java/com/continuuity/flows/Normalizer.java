package com.continuuity.flows;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.meta.Event;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes a log event and extracts dimensions.
 */
public class Normalizer extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  private static final ThreadLocal<Gson> GSON = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT1 = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");
    }
  };

  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT2 = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,S");
    }
  };

  //<pattern>%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n</pattern>
  //2013-10-04 11:04:51,489 - WARN  [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated.
  private static final Pattern LOG_LINE_PATTERN1 =
    Pattern.compile("(\\d{4}+-\\d{2}+-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) - (.{5}+) \\[[^:]+:([^@]+)@\\d+\\] - .*",
                    Pattern.MULTILINE | Pattern.DOTALL);

  //2013-10-03T17:25:08,863Z INFO  c.c.d.t.d.AbstractClientProvider [poorna-logsaver-ha.dev.continuuity.net]
  // [executor-46] AbstractClientProvider:newClient(AbstractClientProvider.java:125) - Connected to tx service at
  private static final Pattern LOG_LINE_PATTERN2 =
    Pattern.compile(
      "(\\d{4}+-\\d{2}+-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3})Z (.{5}+) ([^ ]+) .*",
      Pattern.MULTILINE | Pattern.DOTALL);

  private static final Parser[] PARSERS =
    new Parser[] {new Parser(LOG_LINE_PATTERN1, DATE_FORMAT1), new Parser(LOG_LINE_PATTERN2, DATE_FORMAT2)};

  @SuppressWarnings("UnusedDeclaration")
  private OutputEmitter<Event> eventEmitter;

  /**
   *
   * @param streamEvent of json format {"host" : "abc.net", "component" : "app-fabric",
   *                    "logline" : ""2013-10-04 11:04:51,489 - WARN  [main:o.a.h.c.Configuration@816]..."}
   */
  @SuppressWarnings("UnusedDeclaration")
  @ProcessInput
  public void process(StreamEvent streamEvent) {
    try {
      String jsonStr = Bytes.toString(Bytes.toBytes(streamEvent.getBody()));
      @SuppressWarnings("unchecked")
      Event event = parseEvent((Map<String, String>) GSON.get().fromJson(jsonStr, MAP_TYPE));

      if (event != null) {
        eventEmitter.emit(event);
      }
    } catch (ParseException e) {
      LOG.error("Caught exception: ", e);
    }
  }

  static Event parseEvent(Map<String, String> json) throws ParseException {

    String logLine = json.get("logline");

    for (Parser parser : PARSERS) {
      Pattern pattern = parser.getPattern();
      SimpleDateFormat dateFormat = parser.getDateFormat();
      Matcher matcher = pattern.matcher(logLine);
      if (matcher.matches()) {
        return new Event(dateFormat.parse(matcher.group(1)).getTime(), logLine,
                         ImmutableMap.of("level", matcher.group(2).trim(),
                                         "class", matcher.group(3),
                                         "hostname", json.get("host"),
                                         "component", json.get("component")));
      }
    }

    return null;
  }

  private static class Parser {
    private final Pattern pattern;
    private final ThreadLocal<SimpleDateFormat> dateFormat;

    private Parser(Pattern pattern, ThreadLocal<SimpleDateFormat> dateFormat) {
      this.pattern = pattern;
      this.dateFormat = dateFormat;
    }

    public Pattern getPattern() {
      return pattern;
    }

    public SimpleDateFormat getDateFormat() {
      return dateFormat.get();
    }
  }
}
