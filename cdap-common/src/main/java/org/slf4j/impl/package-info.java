/*
 * Copyright © 2026 Cask Data, Inc.
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

/**
 * Legacy SLF4J 1.7 binder shim for callers such as Spark 3.1/3.3 that reflectively or directly
 * inspect {@code org.slf4j.impl.StaticLoggerBinder} when running with SLF4J 2.x and Logback 1.6.x.
 */
package org.slf4j.impl;
