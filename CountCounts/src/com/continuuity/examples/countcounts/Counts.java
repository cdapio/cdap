package com.continuuity.examples.countcounts;

public class Counts {
  private int wordCount;
  private int lineLength;
  
  public Counts(int wordCount, int lineLength) {
    this.wordCount = wordCount;
    this.lineLength = lineLength;
  }

  public int getWordCount() {
    return wordCount;
  }

  public int getLineLength() {
    return lineLength;
  }
}
