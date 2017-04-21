/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.cli.english;

/**
 * English article.
 */
public abstract class Article {

  public static final Article A = new A();
  public static final Article THE = new The();

  public abstract String toString(Word word);

  /**
   * "a" or "an"
   */
  private static class A extends Article {
    @Override
    public String toString(Word word) {
      if (word.beginsWithVowel()) {
        return "an";
      } else {
        return "a";
      }
    }
  }

  /**
   * "the"
   */
  private static class The extends Article {
    @Override
    public String toString(Word word) {
      return "the";
    }
  }
}
