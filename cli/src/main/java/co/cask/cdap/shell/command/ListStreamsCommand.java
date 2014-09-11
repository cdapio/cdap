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

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.StreamRecord;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Lists streams.
 */
public class ListStreamsCommand extends AbstractCommand {

  private final StreamClient streamClient;

  @Inject
  public ListStreamsCommand(StreamClient streamClient) {
    super("streams", null, "Lists " + ElementType.STREAM.getPluralPrettyName());
    this.streamClient = streamClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    new AsciiTable<StreamRecord>(
      new String[] { "name" },
      streamClient.list(),
      new RowMaker<StreamRecord>() {
        @Override
        public Object[] makeRow(StreamRecord object) {
          return new String[]{object.getId()};
        }
      }).print(output);
  }
}
