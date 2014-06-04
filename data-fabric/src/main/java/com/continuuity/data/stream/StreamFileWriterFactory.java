/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data2.transaction.stream.StreamConfig;

import java.io.IOException;

/**
 *
 */
public interface StreamFileWriterFactory {

  FileWriter<StreamEvent> create(StreamConfig config, int generation) throws IOException;
}
