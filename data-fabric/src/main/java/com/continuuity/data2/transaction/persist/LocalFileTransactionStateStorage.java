package com.continuuity.data2.transaction.persist;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class LocalFileTransactionStateStorage extends InMemoryTransactionStateStorage {
  // TODO: implement this to persist to a directory on the local filesystem
}
