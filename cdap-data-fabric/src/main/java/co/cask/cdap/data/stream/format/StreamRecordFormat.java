/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.stream.format;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.internal.io.RecordFormat;

/**
 * Format for a stream event, that reads a {@link StreamEvent} into another object.
 *
 * @param <T> type of object to read the stream event as.
 */
public abstract class StreamRecordFormat<T> extends RecordFormat<StreamEvent, T> {

}
