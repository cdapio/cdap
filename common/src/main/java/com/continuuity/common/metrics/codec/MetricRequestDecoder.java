package com.continuuity.common.metrics.codec;

import com.continuuity.common.metrics.MetricRequest;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A decoder for converting the command to
 * {@link com.continuuity.common.metrics.MetricRequest}.
 *
 * The decoder receives a byte stream that looks as follows:
 *
 * put <metric-type>:<metric-name> <timestamp> <value> [<tag>[ <tag> ...]]
 *
 * and tag is defined by a key and value.
 * tag := key=value
 *
 * Decode implementation cumulates received buffers to make a message complete
 * or to postone decoding until more buffers arrive.
 */
public class MetricRequestDecoder extends CumulativeProtocolDecoder {
  private static final Logger Log
    = LoggerFactory.getLogger(MetricRequestDecoder.class);

  private static final Splitter CMD_SPLITTER
    = Splitter.onPattern(" +").trimResults().omitEmptyStrings();

  private static final Splitter TAG_SPLITTER
    = Splitter.on(" ").trimResults().omitEmptyStrings();

  private static final Joiner TAG_JOINER
    = Joiner.on(" ");

  private static final String CLUSTER_TYPE
    = System.getenv("CLUSTER_TYPE") != null ?
          System.getenv("CLUSTER_TYPE") :
          "UNKNOWN";

  private static final String CLUSTER_NAME
    = System.getenv("CLUSTER_NAME") != null ?
          System.getenv("CLUSTER_NAME") :
          "UNKNOWN";

  @Override
  protected boolean doDecode(IoSession session, IoBuffer in,
                             ProtocolDecoderOutput out) throws Exception {

    // Remember the initial position.
    int start = in.position();

    // Now find the first CRLF in the buffer.
    byte previous = 0;
    while(in.hasRemaining()) {
      byte current = in.get();
      if(current == '\n') {
        // remember the current position and limit.
        int position = in.position();
        int limit = in.limit();
        try {
          in.position(start);
          in.limit(position);
          // the bytes between in.position and in.limit now contain a full
          // CRLF terminated line.
          out.write(parseCommand(in.slice()));
        } finally {
          // Set the position to point right after the detected line and
          // set the limit to the old one.
          in.position(position);
          in.limit(limit);
        }
        // Decode one line; CumulativeProtocolDecode will be calling us again
        // until we return false. So, just return true until there are no more
        // lines in the buffer.
        return true;
      }
      previous = current;
    }

    // Could not find CRLF in the buffer. Reset the initial position to the
    // one we recorded above.
    in.position(start);

    return false;
  }

  /**
   * Parses the buffer into a MetricRequest as it is a full request
   * that is now available to be decoded.
   *
   * @param slice
   * @return an instance of MetricRequest object.
   */
  private MetricRequest parseCommand(IoBuffer slice) {
    // Instance of invalid metric
    MetricRequest invalidMetric
      = new MetricRequest.Builder(false).create();

    // For now we make a copy, but we can parse the command without
    // making a copy.
    byte[] buffer = new byte[slice.remaining()];

    // Read in the buffer and convert it to string -- it's easy to operate
    // on it.
    slice.get(buffer);
    String command = new String(buffer);

    // Split the command into it's constituents.
    Iterable<String> constituents = CMD_SPLITTER.split(command);

    String cmd = null, metric = null, timestampStr = null,
      valueStr = null, tagsStr = "";
    int idx = 0;
    for(String constituent : constituents) {
      switch(idx) {
        case 0:
            cmd = constituent.toLowerCase();
          break;
        case 1:
            metric = constituent;
          break;
        case 2:
            timestampStr = constituent;
          break;
        case 3:
          valueStr = constituent;
          break;
        default:
          tagsStr = tagsStr + " " + constituent;
      }
      idx++;
    }

    // if any of the fields needed are not populated, then we term the
    // metric as invalid metric. We still pass it along with an indicator
    // set that it's a invalid metric request.
    if(cmd == null || metric == null || timestampStr == null
      || valueStr == null) {
      Log.warn("Request has empty field(s). Command {}", command);
      return invalidMetric;
    }

    // We make more checks to make sure the request is well formed.
    if(! "put".equals(cmd)) {
      Log.warn("Request is not a put metric operation");
      return invalidMetric;
    }

    // Check metric field if it has a type specified by a colon, then
    // extract the type else default it to system and the name to metric.
    // If not extract the type and name of metric.
    String type, name;
    idx = metric.indexOf(":");
    if(idx == -1) {
      type = "system";
      name = metric;
    } else {
      type = metric.substring(0,idx);
      name = metric.substring(idx+1);
    }

    // Convert timestamp to log.
    long timestamp = -1;
    try {
      timestamp = Long.parseLong(timestampStr);
    } catch (NumberFormatException e) {
      Log.warn("invalid timestamp {} passed.", timestampStr);
      return invalidMetric;
    }

    // Convert value to float.
    float value = Float.NaN;
    try {
      value = Float.parseFloat(valueStr);
    } catch (NumberFormatException e) {
      Log.warn("Invalid value {}", valueStr);
      return invalidMetric;
    }

    MetricRequest.Builder requestBuilder =
      new MetricRequest.Builder(true);

    // Add cluster type and cluster name to the tags string.
    tagsStr = String.format("%s %s=%s %s=%s",
                            tagsStr,
                            "cltype", CLUSTER_TYPE,
                            "clname", CLUSTER_NAME
    );

    // Extract tags.
    String rawTags = "";
    if(! tagsStr.isEmpty()) {
      Iterable<String> tags = TAG_SPLITTER.split(tagsStr);
      rawTags = TAG_JOINER.join(tags);
      for(String tag : tags) {
        idx = tag.indexOf("=");
        if(idx != -1) {
          requestBuilder.addTag(tag.substring(0, idx), tag.substring(idx+1));
        }
      }
    }

    // Add all the required fields to the request.
    String rawRequest =
      String.format("%s %s %s %s %s", cmd, name, timestamp, value, rawTags);
    requestBuilder.setRawRequest(rawRequest);
    requestBuilder.setRequestType(cmd);
    requestBuilder.setMetricName(name);
    requestBuilder.setMetricType(type);
    requestBuilder.setTimestamp(timestamp);
    requestBuilder.setValue(value);

    // Create an instance of request and return.
    return requestBuilder.create();
  }
}
