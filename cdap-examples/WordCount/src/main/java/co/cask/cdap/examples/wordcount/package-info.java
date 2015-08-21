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
/**
 * This package contains the WordCount sample Application that counts words and tracks the associations between words.
 * This is a slightly modified version of the classic WordCount example. 
 * 
 * This WordCount Application consists of:
 * 
 * 1. A Stream named wordStream that receives strings of words to be counted.
 * 
 * 2. A Flow named WordCounter processes the strings from the Stream and calculates the word counts
 *    and other word statistics using four Flowlets:
 *    - The splitter splits the input string into words and aggregates and persists global statistics;
 *    - The counter takes words as inputs and calculates and persists per-word statistics;
 *    - The unique Flowlet calculates the unique number of words seen;
 *    - The associator stores word associations between all of the words in each input string.
 *
 * 3. A Service named ``RetrieveCounts`` that serves read requests for calculated statistics,
 *    word counts and associations. It exposes these endpoints:
 *    - ``/stats`` returns the total number of words, the number of unique words, and the average word length;
 *    - ``/count/{word}`` returns the word count of a specified word and its word associations,
 *      up to the specified limit or a pre-set limit of ten if not specified;
 *    - ``/assoc/{word1}/{word2}`` returns the top associated words (those with the highest counts).
 *
 * 4. Four Datasets used by the Flow and Service to model, store, and serve the data:
 *    - A core Table named wordStats to track global word statistics;
 *    - A system KeyValueTable Dataset named wordCounts counts the occurrences of each word;
 *    - A custom UniqueCountTable Dataset named uniqueCount determines and counts the number of unique words seen;
 *    - A custom AssociationTable Dataset named wordAssocs tracks associations between words.
 */
package co.cask.cdap.examples.wordcount;
