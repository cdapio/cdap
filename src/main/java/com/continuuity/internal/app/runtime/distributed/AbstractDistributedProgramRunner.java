/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 *
 */
public abstract class AbstractDistributedProgramRunner implements ProgramRunner {

  protected final WeaveRunner weaveRunner;
  protected final File hConfFile;
  protected final File cConfFile;

  protected AbstractDistributedProgramRunner(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
    this.weaveRunner = weaveRunner;
    try {
      hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
      hConfFile.deleteOnExit();
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private File saveHConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }
}
