package com.continuuity.performance.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.continuuity.performance.benchmark.SimpleBenchmark;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Benchmark that generates stream events by sending REST calls to the Continuuity Gateway.
 */
public class LoadGenerator extends SimpleBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

  private String apikey = null;
  private String hostname;
  private String requestUrl = null;

  private int length = 100;

  private String [] wordList = shortList;

  private Random seedRandom = new Random();

  public LoadGenerator() {
    hostname = null;
  }

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--base <url>", "The base URL of the gateway stream interface.");
    usage.put("--gateway <hostname>", "The hostname of the gateway. The rest " +
        "of the gateway URL will be constructed from the local default " +
        "configuration. Default is localhost.");
    usage.put("--stream <name>", "The name of the stream to send events to.");
    usage.put("--words <filename>", "To specify a file containing a " +
        "vocabulary for generating the random text, one word per line. ");
    usage.put("--length <num>", "To specify a length limit for the generated " +
        "random text. Default is 100 words.");
    return usage;
  }

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {

    super.configure(config);

    String apikey = config.get("apikey");
    String baseUrl = config.get("base");
    hostname = config.get("gateway");
    String destination = config.get("stream");
    String file = config.get("words");
    length = config.getInt("length", length);
    boolean ssl = apikey != null;

    // determine the base url for the GET request
    if (baseUrl == null) {
      // todo
      baseUrl = "Perf framework should be fixed towards new gateway";
      // baseUrl = Util.findBaseUrl(config, RestCollector.class, null, hostname, -1, ssl);
    }

    Preconditions.checkNotNull(baseUrl, "Can't figure out gateway URL. Please specify --base");

    LOG.info("Base Url = {}", baseUrl);

    if (super.simpleConfig.verbose) {
        System.out.println("Using base URL: " + baseUrl);
    }

    Preconditions.checkNotNull(destination, "Destination stream must be specified via --stream");

    requestUrl = baseUrl + destination;

    LOG.info("Request Url = {}", requestUrl);

    if (file != null) {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        List<String> words = Lists.newArrayList();
        while ((line = reader.readLine()) != null) {
          words.add(line);
        }
        reader.close();
        wordList = words.toArray(new String[words.size()]);
        System.out.println("Using word list in " + file + " (" + words.size() + " words).");
      } catch (IOException e) {
        throw new BenchmarkException("Cannot read word list from file " + file + ": " + e.getMessage(), e);
      }
    } else {
      System.out.println("Using built-in word list of 100 most frequent " + "english words.");
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
          public Agent newAgent(int agentId, final int numAgents) {
            return new Agent(agentId) {
              @Override
              public long runOnce(long iteration) throws BenchmarkException {

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
                  System.out.println(getName() + " " + this.getAgentId() + " sending event number " + iteration
                                       + " with body: " + body);
                }

                // create an HttpPost
                HttpPost post = new HttpPost(requestUrl);
                if (apikey != null) {
                  post.addHeader(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
                }
                byte[] binaryBody = body.getBytes();
                post.setEntity(new ByteArrayEntity(binaryBody));

                // post is now fully constructed, ready to send
                // prepare for HTTP
                try {
                  HttpClient client = new DefaultHttpClient();
                  HttpResponse response = client.execute(post);
                  if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    throw new BenchmarkException("Unexpected HTTP response: " + response.getStatusLine());
                  }
                  client.getConnectionManager().shutdown();
                } catch (IOException e) {
                  throw new BenchmarkException("Error sending HTTP request: " + e.getMessage(), e);
                }
                return 1L;
              }
            };
          } // newAgent()
        } // new AgentGroup()
    }; // new AgentGroup[]
  } // getAgentGroups()


  public static void main(String[] args) throws Exception {
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
