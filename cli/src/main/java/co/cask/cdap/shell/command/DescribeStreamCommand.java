/*
 * Copyright 2012-2014 Cask Data, Inc.
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

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.element.StreamIdCompleter;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Shows detailed information about a stream.
 */
public class DescribeStreamCommand extends AbstractCommand implements Completable {

  private final StreamClient streamClient;
  private final StreamIdCompleter completer;

  @Inject
  public DescribeStreamCommand(StreamIdCompleter completer, StreamClient streamClient) {
    super("stream", "<stream-id>", "Shows detailed information about a " + ElementType.STREAM.getPrettyName());
    this.completer = completer;
    this.streamClient = streamClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String streamId = args[0];
    StreamProperties config = streamClient.getConfig(streamId);

    new AsciiTable<StreamProperties>(
      new String[] { "name", "ttl"},
      Lists.newArrayList(config),
      new RowMaker<StreamProperties>() {
        @Override
        public Object[] makeRow(StreamProperties object) {
          return new Object[] { object.getName(), object.getTTL() };
        }
      }
    ).print(output);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
