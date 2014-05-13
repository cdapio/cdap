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
 * Package for the CountTokens sample application.
 * 
 * Reads events (= byte[] body, Map<String,String> headers) from the 'text' input Stream.
 * 
 * Tokenizes the text in the body and in the header named 'title', ignores all other headers.
 * 
 * Each token is cloned into two tokens:
 *   a) the upper case version of the token
 *   b) the original token with a field prefix 'title',
 *   or 'text' if the token is from the body of the event.
 *
 * All of the cloned tokens are counted using increment operations.
 */
package com.continuuity.examples.counttokens;


