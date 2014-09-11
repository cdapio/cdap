/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.element.StreamIdCompleter;
import co.cask.cdap.shell.exception.CommandInputError;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A CLI command for getting stream events.
 */
public class GetStreamEventsCommand extends AbstractCommand implements Completable {

  private static final int MAX_BODY_SIZE = 256;
  private static final int LINE_WRAP_LIMIT = 64;
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final StreamClient streamClient;
  private final StreamIdCompleter completer;

  @Inject
  public GetStreamEventsCommand(StreamClient streamClient, StreamIdCompleter completer) {
    super(
      "stream", "<stream-id> [<start-time> <end-time> <limit>]",
      "Gets events from " + ElementType.STREAM.getPrettyName() + ". " +
      "The time format for <start-time> and <end-time> could be timestamp in milliseconds or " +
      "relative time in the form of [+\\-][0-9]+[hms]. " +
      "For <start-time>, it is relative to current time, " +
      "while for <end-time>, it's relative to start time. " +
      "Special constants \"min\" and \"max\" can also be used to represent 0 and max timestamp respectively."
     );

    this.streamClient = streamClient;
    this.completer = completer;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    if (args.length < 1) {
      throw new CommandInputError("Expected arguments: " + argsFormat);
    }

    // Defaults for start time, end time, and limit if they are not provided.
    long startTime = 0L;
    long endTime = Long.MAX_VALUE;
    int limit = Integer.MAX_VALUE;

    if (args.length > 1) {
      startTime = getTimestamp(args[1], System.currentTimeMillis());
    }
    if (args.length > 2) {
      endTime = getTimestamp(args[2], startTime);
    }
    if (args.length > 3) {
      limit = Integer.parseInt(args[3]);
    }

    // Get a list of stream events and prints it.
    List<StreamEvent> events = streamClient.getEvents(args[0], startTime, endTime,
                                                      limit, Lists.<StreamEvent>newArrayList());
    new AsciiTable<StreamEvent>(
      new String[] { "timestamp", "headers", "body size", "body"},
      events,
      new RowMaker<StreamEvent>() {
        @Override
        public Object[] makeRow(StreamEvent event) {
          long bodySize = event.getBody().remaining();

          return new Object[] {
            event.getTimestamp(),
            event.getHeaders().isEmpty() ? "" : formatHeader(event.getHeaders()),
            bodySize,
            getBody(event.getBody())
          };
        }
      }
    ).print(output);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return ImmutableList.of(
      prefixCompleter(prefix, completer)
    );
  }

  /**
   * Returns a timestamp in milliseconds.
   *
   * @param arg The string argument user provided.
   * @param base The base timestamp to relative from if the time format provided is a relative time.
   * @return Timestamp in milliseconds
   * @throws CommandInputError if failed to parse input.
   */
  private long getTimestamp(String arg, long base) {
    try {
      if (arg.startsWith("+") || arg.startsWith("-")) {
        int dir = arg.startsWith("+") ? 1 : -1;
        char type = arg.charAt(arg.length() - 1);
        int offset = Integer.parseInt(arg.substring(1, arg.length() - 1));
        switch (type) {
          case 's':
            return base + dir * TimeUnit.SECONDS.toMillis(offset);
          case 'm':
            return base + dir * TimeUnit.MINUTES.toMillis(offset);
          case 'h':
            return base + dir * TimeUnit.HOURS.toMillis(offset);
          case 'd':
            return base + dir * TimeUnit.DAYS.toMillis(offset);
          default:
            throw new CommandInputError("Unsupported time type " + type);
        }
      }
      if (arg.equalsIgnoreCase("min")) {
        return 0L;
      }
      if (arg.equalsIgnoreCase("max")) {
        return Long.MAX_VALUE;
      }

      return Long.parseLong(arg);
    } catch (NumberFormatException e) {
      throw new CommandInputError("Invalid number value: " + arg + ". Reason: " + e.getMessage());
    }
  }

  /**
   * Creates a string representing the output of stream event header. Each key/value pair is outputted on its own
   * line in the form {@code <key> : <value>}.
   */
  private String formatHeader(Map<String, String> headers) {
    StringBuilder builder = new StringBuilder();
    String separator = "";
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      builder.append(separator).append(entry.getKey()).append(" : ").append(entry.getValue());
      separator = LINE_SEPARATOR;
    }
    return builder.toString();
  }

  /**
   * Creates a string representing the body in the output. It only prints up to {@link #MAX_BODY_SIZE}, with line
   * wrap at each {@link #LINE_WRAP_LIMIT} character.
   */
  private String getBody(ByteBuffer body) {
    ByteBuffer bodySlice = body.slice();
    boolean hasMore = false;
    if (bodySlice.remaining() > MAX_BODY_SIZE) {
      bodySlice.limit(MAX_BODY_SIZE);
      hasMore = true;
    }

    String str = Bytes.toStringBinary(bodySlice) + (hasMore ? "..." : "");
    if (str.length() <= LINE_WRAP_LIMIT) {
      return str;
    }
    return Joiner.on(LINE_SEPARATOR).join(Splitter.fixedLength(LINE_WRAP_LIMIT).split(str));
  }
}
