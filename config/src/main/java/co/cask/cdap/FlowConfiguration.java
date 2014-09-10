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

package co.cask.cdap;

import java.util.HashMap;

public class FlowConfiguration implements Configuration {
  private HashMap<String, String> config;

  public FlowConfiguration() {
    this.config = new HashMap<String, String>();
  }

  @Override
  public void create(String key, String value) {

  }

  @Override
  public void read(String key, String def) {

  }

  @Override
  public void update(String key, String old_value, String new_value) {

  }

  @Override
  public void delete(String key) {

  }

  @Override
  public void keys(String key, String def) {

  }
}
