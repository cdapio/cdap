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
 * Package for HelloWorld sample Application.
 * 
 * This is a simple HelloWorld example that uses one Stream, one DataSet, one Flow and one Procedure.
 * <uL>
 *   <li>A Stream to send names to.</li>
 *   <li>A Flow with a single Flowlet that reads the Stream and stores each name in a KeyValueTable</li>
 *   <li>A Procedure that reads the name from the KeyValueTable and prints 'Hello [Name]!'</li>
 * </uL>
 */
package com.continuuity.examples.helloworld;


