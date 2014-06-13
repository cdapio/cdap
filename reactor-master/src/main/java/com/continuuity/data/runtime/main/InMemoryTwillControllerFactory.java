package com.continuuity.data.runtime.main;

import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogHandler;

/**
 *
 */
interface InMemoryTwillControllerFactory {
  InMemoryTwillController create(RunId runId, Iterable<LogHandler> logHandlers);
}
