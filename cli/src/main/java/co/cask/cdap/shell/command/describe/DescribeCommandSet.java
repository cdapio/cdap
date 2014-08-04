/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command.describe;

import co.cask.cdap.shell.command.CommandSet;

import javax.inject.Inject;

/**
 * Contains commands for describe stuff.
 */
public class DescribeCommandSet extends CommandSet {

  @Inject
  public DescribeCommandSet(DescribeAppCommand describeAppCommand,
                            DescribeDatasetCommandSet describeDatasetCommandSet) {
    super("describe", describeAppCommand, describeDatasetCommandSet);
  }
}
