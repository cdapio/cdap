package com.example;

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
