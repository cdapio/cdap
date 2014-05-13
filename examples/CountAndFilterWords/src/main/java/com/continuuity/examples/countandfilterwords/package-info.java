/**
 * Copyright 2013-2014 Continuuity, Inc.
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
 * This package contains the CountAndFilterWords sample application,
 * a variation of CountTokens that illustrates that a Flowlet's output can
 * be consumed by multiple downstream Flowlets.
 *
 * In addition to counting all tokens, CountAndFilterWords sends all tokens to a filter that
 * drops all tokens that are not upper case.
 *
 * The upper case tokens are then counted by a separate Flowlet.
 */
package com.continuuity.examples.countandfilterwords;


