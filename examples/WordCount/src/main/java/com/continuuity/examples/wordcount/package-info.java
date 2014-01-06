/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * This package contains the WordCount sample application that counts words and tracks the associations between words.
 * This is a slightly modified version of the classic WordCount example. 
 * 
 * This WordCount application consists of:
 * 
 * 1. A stream named wordStream that receives strings of words to be counted.
 * 
 * 2. A flow named WordCounter processes the strings from the stream and calculates the word counts
 * and other word statistics using four flowlets:
 *    - The splitter splits the input string into words and aggregates and persists global statistics.
 *    - The counter takes words as inputs and calculates and persists per-word statistics.
 *    - The unique flowlet calculates the unique number of words seen.
 *    - The associator stores word associations between all of the words in each input string.
 *
 * 3. A procedure named RetrieveCounts serves read requests for the calculated word counts,
 * statistics and associations. It supports two methods:
 *    - getCount() accesses the word count of a specified word and its word associations.
 *    - getStats() accesses the global word statistics.
 *
 * 4. Four datasets used by the flow and query to model, store, and serve the necessary data:
 *    - A core Table named wordStats to track global word statistics.
 *    - A system KeyValueTable dataset named wordCounts counts the occurrences of each word.
 *    - A custom UniqueCountTable dataset named uniqueCount determines and counts the number of unique words seen.
 *    - A custom AssociationTable dataset named wordAssocs tracks associations between words.
 */
package com.continuuity.examples.wordcount;


