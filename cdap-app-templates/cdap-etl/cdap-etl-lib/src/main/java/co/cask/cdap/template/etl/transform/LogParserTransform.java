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

package co.cask.cdap.template.etl.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.TransformContext;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Parses log formats to extract access information
 */
@Plugin(type = "transform")
@Name("LogParser")
@Description("Parses logs from any input source for relevant information such as URI, IP, Browser, Device, and " +
  "Timestamp.")
public class LogParserTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Schema LOG_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("uri", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ip", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("browser", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("device", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG))
  );
  private static final String LOG_FORMAT_DESCRIPTION = "Log format to parse. Currently supports S3, " +
    "CLF, and Cloudfront formats.";
  private static final String INPUT_NAME_DESCRIPTION = "Name of the field in the input schema which encodes the " +
    "log information. The given field must be of type String or Bytes";
  private static final Logger LOG = LoggerFactory.getLogger(LogParserTransform.class);
  //Regex used to parse a CLF log, each field is commented above
  private static final Pattern CLF_LOG_PATTERN = Pattern.compile(
    //   IP                    id    user      date          request     code     size    referrer    user agent
    "^([\\d.]+|[:][:][\\d]) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) ([-\"\\d]+) \"([^\"]+)\" \"([^\"]+)\"");
  //Regex used to parse a S3 log, each field is commented above
  private static final Pattern S3_LOG_PATTERN = Pattern.compile(
    // bucket owner name   time           ip                    req   reqID operation  key    request
    "^(\\S+) (\\S+) \\[(\\p{Print}+)\\] ([\\d.]+|[:][:][\\d]) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]+)\" " +
      //  HTTP stat error code  bytes sent     obj size  time    turn time  referrer         user agent
      "(\\d{3}) (\\p{Print}+) ([-\"\\d]+) ([-\"\\d]+) ([\\d]+) ([-\"\\d]+) \"(\\p{Print}+)\" \"([^\"]+)\" " +
      //  version id
      "(\\p{Print}+)");
  //Regex used to parse the request field for the URI
  private static final Pattern REQUEST_PAGE_PATTERN = Pattern.compile("(\\S+)\\s(\\S+).*");
  //Indices of which group request, time, ip, and user agent are in the S3 regex
  private static final int[] S3_INDICES = {9, 3, 4, 17};
  //Indices of which group request, time, ip, and user agent are in the CLF regex
  private static final int[] CLF_INDICES = {5, 4, 1, 9};
  //Number of groups matched in the S3 regex
  private static final int S3_REGEX_LENGTH = 18;
  //Number of groups matched in the CLF regex
  private static final int CLF_REGEX_LENGTH = 9;
  private static final String S3_LOG = "S3";
  private static final String CLF_LOG = "CLF";
  private static final String CLOUDFRONT_LOG = "Cloudfront";
  private final LogParserConfig config;
  private final SimpleDateFormat sdfStrftime = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
  private final SimpleDateFormat sdfCloudfront = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss z");

  public LogParserTransform(LogParserConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    if (!S3_LOG.equals(config.logFormat) && !CLF_LOG.equals(config.logFormat) &&
        !CLOUDFRONT_LOG.equals(config.logFormat)) {
      LOG.error("Log format not currently supported.");
      throw new IllegalStateException("Unsupported log format: " + config.logFormat);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String log = getLog(input);
    if (log == null) {
      LOG.debug("Couldn't read schema, log message was null");
      return;
    }

    StructuredRecord output;
    if (S3_LOG.equals(config.logFormat)) {
      Matcher logMatcher = S3_LOG_PATTERN.matcher(log);
      if (!logMatcher.matches() || logMatcher.groupCount() < S3_REGEX_LENGTH) {
        LOG.debug("Couldn't parse log because log did not match the S3 format, log: {}", log);
        return;
      }
      output = parseRequest(logMatcher, S3_INDICES);
    } else if (CLF_LOG.equals(config.logFormat)) {
      Matcher logMatcher = CLF_LOG_PATTERN.matcher(log);
      if (!logMatcher.matches() || logMatcher.groupCount() < CLF_REGEX_LENGTH) {
        LOG.debug("Couldn't parse log because the log did not match the CLF format. log: {}", log);
        return;
      }
      output = parseRequest(logMatcher, CLF_INDICES);
    } else {
      if (log.startsWith("#")) {
        LOG.trace("Log is a comment. Ignoring...");
        return;
      }

      String[] fields = log.split("\\t");
      String uri = fields[7];
      String ip = fields[4];
      long ts = sdfCloudfront.parse(String.format("%s:%s UTC", fields[0], fields[1])).getTime();
      UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
      ReadableUserAgent userAgent = parser.parse(fields[10]);
      String browser = userAgent.getFamily().getName();
      String device = userAgent.getDeviceCategory().getCategory().getName();

      output = StructuredRecord.builder(LOG_SCHEMA)
        .set("uri", uri)
        .set("ip", ip)
        .set("browser", browser)
        .set("device", device)
        .set("ts", ts)
        .build();
    }

    if (output != null) {
      emitter.emit(output);
    }
  }

  /**
   * Gets the log message from the input
   * @param input the StructuredRecord to extract the log from
   * @return the log message, or null on failure
   */
  @Nullable
  private String getLog(StructuredRecord input) {
    Schema.Field inputField = input.getSchema().getField(config.inputName);
    if (inputField == null) {
      LOG.debug("Invalid inputName, no known inputField matches given input of " + config.inputName);
      return null;
    }

    Schema inputSchema = inputField.getSchema();
    if (inputSchema.isNullableSimple()) {
      inputSchema = inputSchema.getNonNullable();
    }
    Schema.Type inputType = inputSchema.getType();

    if (!Schema.Type.STRING.equals(inputType) && !Schema.Type.BYTES.equals(inputType)) {
      LOG.error("Unsupported inputType in schema, only Schema.Type.BYTES and Schema.Type.STRING are supported. " +
                  "InputType: {}", inputType.toString());
      return null;
    }

    if (Schema.Type.STRING.equals(inputType)) {
      return input.get(config.inputName);
    } else {
      Object data = input.get(config.inputName);
      if (data instanceof ByteBuffer) {
        return Bytes.toString((ByteBuffer) input.get(config.inputName));
      } else if (data instanceof byte[]) {
        return Bytes.toString((byte[]) input.get(config.inputName));
      } else {
        LOG.debug("Not a byte type, type is {}", data.getClass().toString());
        return null;
      }
    }
  }

  /**
   * Parses a request for the URI, IP, Browser, Device, and Time
   * @param logMatcher the regex matcher to use
   * @param indices array of indices that define what position in the regex the fields are,
   *                 in the order of Request, Time, IP, and User Agent
   */
  @Nullable
  private StructuredRecord parseRequest(Matcher logMatcher, int[] indices) {
    String request = logMatcher.group(indices[0]);
    Matcher requestMatcher = REQUEST_PAGE_PATTERN.matcher(request);
    if (!requestMatcher.matches() || requestMatcher.groupCount() < 2) {
      LOG.debug("Couldn't parse uri because request does not match request pattern, request: {}", request);
      return null;
    }

    String uri = requestMatcher.group(2);
    long ts = System.currentTimeMillis();
    try {
      ts = sdfStrftime.parse(logMatcher.group(indices[1])).getTime();
    } catch (ParseException e) {
      LOG.debug("Couldn't parse time from the input record, using current timestamp instead. Exception: {}",
                e.getMessage());
    }

    String ip = logMatcher.group(indices[2]);
    UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
    ReadableUserAgent userAgent = parser.parse(logMatcher.group(indices[3]));
    String browser = userAgent.getFamily().getName();
    String device = userAgent.getDeviceCategory().getCategory().getName();

    return StructuredRecord.builder(LOG_SCHEMA)
      .set("uri", uri)
      .set("ip", ip)
      .set("browser", browser)
      .set("device", device)
      .set("ts", ts)
      .build();
  }

  /**
   * Config class for LogParserTransform
   */
  public static class LogParserConfig extends PluginConfig {
    @Name("logFormat")
    @Description(LOG_FORMAT_DESCRIPTION)
    private String logFormat;

    @Name("inputName")
    @Description(INPUT_NAME_DESCRIPTION)
    private String inputName;

    public LogParserConfig(String logFormat, String inputName) {
      this.logFormat = logFormat;
      this.inputName = inputName;
    }
  }
}
