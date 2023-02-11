/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * The request class to push a list of applications to linked git repository.
 */
public class PushAppsRequest {
  private final String commitMessage;
  private final Collection<App> apps;

  public PushAppsRequest(String commitMessage, Collection<App> apps) {
    this.commitMessage = commitMessage;
    this.apps = Collections.unmodifiableList(new ArrayList<>(apps));
  }

  public String getCommitMessage() {
    return commitMessage;
  }

  public Collection<App> getApps() {
    return apps;
  }

  public static class App {
    private final String name;
    private final String version;

    public App(String name, String version) {
      this.name = name;
      this.version = version;
    }

    public String getName() {
      return name;
    }
    
    public String getVersion() {
      return version;
    }
  }
}
