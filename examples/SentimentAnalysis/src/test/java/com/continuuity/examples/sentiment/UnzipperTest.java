/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.sentiment;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;

/**
 *
 */
public class UnzipperTest {
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testUnzip() throws Exception {
    InputStream in = getClass().getResourceAsStream("/sentiment-process.zip");
    Assert.assertNotNull(in);


    File tempFolder = temporaryFolder.newFolder();
    Unzipper.unzip(in, tempFolder);

    File sentimentPy = new File(tempFolder, "sentiment/score_sentiment.py");
    Assert.assertTrue(sentimentPy.exists());
  }
}
