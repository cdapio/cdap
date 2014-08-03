/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api;

/**
 * Process with a Processor.
 * <p>
 *   Processors can be real-time, batch, or non-interactive. Processors
 *   are capable of subscribing to Streams and taking DataSets as inputs. Processors are generally
 *   run within containers.
 * </p>
 */
public interface Processor {

}
