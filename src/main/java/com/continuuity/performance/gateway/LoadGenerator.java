package com.continuuity.performance.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import com.continuuity.performance.benchmark.*;
import com.google.common.collect.Lists;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class LoadGenerator extends SimpleBenchmark {

  String hostname = null;
  String baseUrl = null;
  String destination = null;
  String requestUrl = null;
  String file = null;
  int length = 100;

  String [] wordList = shortList;

  Random seedRandom = new Random();

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {

    super.configure(config);

    baseUrl = config.get("base");
    hostname = config.get("gateway");
    destination = config.get("stream");
    file = config.get("words");
    length = config.getInt("length", length);

    // determine the base url for the GET request
    if (baseUrl == null) baseUrl =
        Util.findBaseUrl(config, RestCollector.class, null, hostname);
    if (baseUrl == null) {
      throw new BenchmarkException(
          "Can't figure out gateway URL. Please specify --base");
    } else {
      if (super.simpleConfig.verbose)
        System.out.println("Using base URL: " + baseUrl);
    }

    if (destination == null)
      throw new BenchmarkException(
          "Destination stream must be specified via --stream");
    else
      requestUrl = baseUrl + destination;

    if (file != null) {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        List<String> words = Lists.newArrayList();
        while ((line = reader.readLine()) != null) {
          words.add(line);
        }
        reader.close();
        wordList = words.toArray(new String[words.size()]);
        System.out.println("Using word list in " + file + " (" + words.size()
            + " words).");
      } catch (IOException e) {
        throw new BenchmarkException("Cannot read word list from file " +
            file + ": " + e.getMessage(), e);
      }
    } else {
      System.out.println("Using built-in word list of 100 most frequent " +
          "english words.");
    }
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[] {
        new SimpleAgentGroup(super.simpleConfig) {
          @Override
          public String getName() {
            return "event generator";
          }

          @Override
          public Agent newAgent() {
            return new Agent() {
              @Override
              public void runOnce(int iteration, int agentId, int numAgents)
                  throws BenchmarkException {

                // create a string of random words and length
                Random rand = new Random(seedRandom.nextLong());
                int numWords = rand.nextInt(length);
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < numWords; i++) {
                  int j = rand.nextInt(wordList.length);
                  String word = wordList[j];
                  if (rand.nextInt(7) == 0) {
                    word = word.toUpperCase();
                  }
                  builder.append(word);
                  builder.append(' ');
                }
                String body = builder.toString();

                if (isVerbose()) {
                  System.out.println(getName() + " " + agentId +
                      " sending event number " + iteration + " with body: " +
                      body);
                }

                // create an HttpPost
                HttpPost post = new HttpPost(requestUrl);
                byte[] binaryBody = body.getBytes();
                post.setEntity(new ByteArrayEntity(binaryBody));

                // post is now fully constructed, ready to send
                // prepare for HTTP
                try {
                  HttpClient client = new DefaultHttpClient();
                  HttpResponse response = client.execute(post);
                  if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                    System.err.println(
                        "Unexpected HTTP response: " + response.getStatusLine());
                  client.getConnectionManager().shutdown();
                } catch (IOException e) {
                  System.err.println("Error sending HTTP request: " + e.getMessage());
                }
              }
            };
          } // newAgent()
        } // new AgentGroup()
    }; // new AgentGroup[]
  } // getAgentGroups()


  public static void main(String[] args) {
    args = Arrays.copyOf(args, args.length + 2);
    args[args.length - 2] = "--bench";
    args[args.length - 1] = LoadGenerator.class.getName();
    BenchmarkRunner.main(args);
  }

  static String[] shortList = {
      "be",
      "to",
      "of",
      "and",
      "a",
      "in",
      "that",
      "have",
      "I",
      "it",
      "for",
      "not",
      "on",
      "with",
      "he",
      "as",
      "you",
      "do",
      "at",
      "this",
      "but",
      "his",
      "by",
      "from",
      "they",
      "we",
      "say",
      "her",
      "she",
      "or",
      "an",
      "will",
      "my",
      "one",
      "all",
      "would",
      "there",
      "their",
      "what",
      "so",
      "up",
      "out",
      "if",
      "about",
      "who",
      "get",
      "which",
      "go",
      "me",
      "when",
      "make",
      "can",
      "like",
      "time",
      "no",
      "just",
      "him",
      "know",
      "take",
      "people",
      "into",
      "year",
      "your",
      "good",
      "some",
      "could",
      "them",
      "see",
      "other",
      "than",
      "then",
      "now",
      "look",
      "only",
      "come",
      "its",
      "over",
      "think",
      "also",
      "back",
      "after",
      "use",
      "two",
      "how",
      "our",
      "work",
      "first",
      "well",
      "way",
      "even",
      "new",
      "want",
      "because",
      "any",
      "these",
      "give",
      "day",
      "most"
  };
}
