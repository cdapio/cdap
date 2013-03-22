/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import java.util.List;

public interface BatchReadable<KEY, VALUE> {
  List<Split> getSplits();

  SplitReader<KEY, VALUE> createSplitReader(Split split);
}
