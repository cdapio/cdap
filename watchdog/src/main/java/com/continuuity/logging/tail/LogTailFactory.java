package com.continuuity.logging.tail;

import com.continuuity.common.conf.CConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory to create LogTail objects.
 */
public interface LogTailFactory {
  LogTail create(CConfiguration cConfig, Configuration hConfig);
}
