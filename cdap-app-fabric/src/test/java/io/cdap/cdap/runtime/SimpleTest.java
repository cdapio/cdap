/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime;

import io.cdap.cdap.internal.app.runtime.distributed.launcher.LauncherRunner;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;

import java.io.File;

/**
 *
 */
public class SimpleTest {
  public static void main(String[] args) throws Exception {
    String bundleJarName = "cdap.jar";

    LocationFactory factory = new LocalLocationFactory(new File("/Users/vinishashah/Desktop/fromcdap"));
    Location tmp = factory.create("jar");
    ApplicationBundler bundler = new ApplicationBundler(new ClassAcceptor());
    bundler.createBundle(tmp.append("cdap.jar"), LauncherRunner.class);

    System.out.println("done");

    System.out.println("Jar with wrapper launcher class has been created.");
  }
}
