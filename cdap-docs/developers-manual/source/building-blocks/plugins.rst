.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _plugins:

=======
Plugins
=======

.. highlight:: java

A *Plugin* is a Java class that extends an application class by implementing an interface expected by the
application class. *Plugins* can be packaged in a separate artifact from the application class that uses it.

.. rubric:: Example Use Case

When writing an application class, it is often useful to create interfaces or abstract classes that define
a logical contract in your code, but do not provide an implementation of that contract. This lets you plug in
different implementations to fit your needs.

For example, consider the classic word count example for MapReduce. The program reads files, tokenizes lines
in those files into words, and then counts how many times each word appears. The code consists of several classes::

  public class WordCountApp extends AbstractApplication {

    @Override
    public void configure() {
      addMapReduce(new WordCount());
    }
  }

  public static class WordCount extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(WordCountMapper.class);
      job.setReducerClass(WordCountReducer.class);
      // setup input and output
    }
  }

  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, ONE);
      }
    }
  }

  public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text word, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }
      context.write(word, new LongWritable(sum));
    }
  }

We package our code into a JAR file named ``wordcount-1.0.0.jar`` and add it to CDAP::

  curl localhost:10000/v3/namespaces/default/artifacts/wordcount --data-binary @wordcount-1.0.0.jar

We then create an application from that artifact::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/basicwordcount -H 'Content-Type: application/json' -d '
  {
    "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" }
  }'

This program runs just fine. It counts all words in the input. However, what if we want to count phrases
instead of words? Or what if we want to filter out common words such as 'the' and 'a'? We would not want
to copy and paste our application class and then make just small tweaks.

Instead, we would like to be able to create applications that
are configured to tokenize the line in different ways. That is, if we want an application that filters
stopwords, we want to be able to create it through a configuration::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/stopwordcount -H 'Content-Type: application/json' -d '
  {
    "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" },
    "config": { "tokenizer": "stopword" }
  }'

Similarly, we want to be able to create an application that counts phrases through a configuration::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/phrasecount -H 'Content-Type: application/json' -d '
  {
    "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" },
    "config": { "tokenizer": "phrase" }
  }'

This is possible by changing our code to use the *Plugin* framework. The first thing we need to do is
introduce a ``Tokenizer`` interface::

  public interface Tokenizer {
    Iterable<String> tokenize(String line);
  }

Now we change our ``WordCountMapper`` to use the plugin framework to instantiate and use a ``Tokenizer``::

  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    implements ProgramLifecycle<MapReduceContext> {
    private static final LongWritable ONE = new LongWritable(1);
    private Text word = new Text();
    private Tokenizer tokenizer;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      for (String token : tokenizer.tokenize(line)) {
        word.set(token);
        context.write(word, ONE);
      }
    }

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      tokenizer = context.newPluginInstance("tokenizerId");
    }

    @Override
    public void destroy() {
      //no-op
    }
  }

The key method we added was the ``initialize`` method. In it, we are using CDAP's plugin framework
to instantiate a plugin of type ``Tokenizer``, identified by ``tokenizerId``. This code runs when
the MapReduce program runs. In order for CDAP to know which plugin ``tokenizerId`` refers to, we will need
to register the plugin in our application's ``configure`` method. We change our application code to
use a configuration object that will specify the name of the ``Tokenizer`` to use, and register that plugin::

  public class WordCountApp extends AbstractApplication<WordCountApp.TokenizerConfig> {

    public static class TokenizerConfig extends Config {
      private String tokenizer;
    }

    @Override
    public void configure() {
      TokenizerConfig config = getConfig();
      // usePlugin(type, name, id, properties)
      usePlugin("tokenizer", config.tokenizer, "tokenizerId", PluginProperties.builder().build());
      addMapReduce(new WordCount());
    }
  }

CDAP will take whatever is specified in the ``config`` section of the application creation
request and convert it into the ``Config`` object expected by the application class.
If it receives this request::

  {
    "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" },
    "config": { "tokenizer": "phrase" }
  }

the ``TokenizerConfig`` will have its ``tokenizer`` field set to ``phrase``.

This allows us to configure which tokenizer should be used when creating an application.
Since we want other artifacts to implement the ``Tokenizer`` interface, we need to make
sure the class is exposed to other artifacts. We do this by including the ``Tokenizer``'s package
in the ``Export-Package`` manifest attribute of our JAR file. For example, if our ``Tokenizer`` full
class name is ``com.example.api.Tokenizer``, we need to expose the ``com.example.api``
package in our pom::

        <plugin>
          <groupId>org.apache.felix</groupId>
          <artifactId>maven-bundle-plugin</artifactId>
          <version>2.3.7</version>
          <extensions>true</extensions>
          <configuration>
            <archive>
              <manifest>
                <mainClass>${app.main.class}</mainClass>
              </manifest>
            </archive>
            <instructions>
              <Embed-Dependency>*;inline=false;scope=compile</Embed-Dependency>
              <Embed-Transitive>true</Embed-Transitive>
              <Embed-Directory>lib</Embed-Directory>
              <Export-Package>com.example.api</Export-Package>
            </instructions>
          </configuration>
          ...
        </plugin>

We then package the code in a new version of the artifact ``wordcount-1.1.0.jar`` and deploy it::

  curl localhost:10000/v3/namespaces/default/artifacts/wordcount --data-binary @wordcount-1.1.0.jar

Finally, we need to implement some tokenizer plugins. *Plugins* are just Java classes that have
been annotated with a plugin type and name::

  @Plugin(type = "tokenizer")
  @Name("default")
  public class DefaultTokenizer implements Tokenizer {

    @Override
    public Iterable<String> tokenize(String line) {
      return Splitter.on(' ').split(line);
    }
  }

  @Plugin(type = "tokenizer")
  @Name("stopword")
  public static class StopWordTokenizer implements Tokenizer {
    private static final Set<String> STOPWORDS = StopWords.load();

    @Override
    public Iterable<String> tokenize(String line) {
      List<String> tokens = new ArrayList<>();
      for (String word : Splitter.on(' ').split(line)) {
        if (!STOPWORDS.contains(word)) {
          tokens.add(word);
        }
      }
      return tokens;
    }
  }

  @Plugin(type = "tokenizer")
  @Name("phrase")
  public static class PhraseTokenizer implements Tokenizer {

    @Override
    public Iterable<String> tokenize(String line) {
      List<String> tokens = new ArrayList<>();
      Iterator<String> wordIter = Splitter.on(' ').split(line).iterator();
      if (!wordIter.hasNext()) {
        return tokens;
      }
      String prevWord = wordIter.next();
      while (wordIter.hasNext()) {
        String currWord = wordIter.next();
        tokens.add(prevWord + " " + currWord);
        prevWord = currWord;
      }
      return tokens;
    }
  }

We package these tokenizers in a separate artifact named ``tokenizers-1.0.0.jar``. When deploying
this artifact, we tell CDAP that the artifact extends the ``wordcount`` artifact, versions
``1.1.0`` inclusive to ``2.0.0`` exclusive::

  curl localhost:10000/v3/namespaces/default/artifacts/tokenizers --data-binary @tokenizers-1.0.0.jar -H 'Artifact-Extends:wordcount[1.1.0,2.0.0)'

This will make the plugins available to those versions of the ``wordcount`` artifact. We can now create
applications that use the tokenizer we want::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/phrasecount -H 'Content-Type: application/json' -d '
  {
    "artifact": { "name": "wordcount", "version": "1.1.0", "scope": "user" },
    "config": { "tokenizer": "phrase" }
  }'

.. rubric:: Plugin Config

*Plugins* can also be configured using the *PluginConfig* class. Suppose we want to modify our
``DefaultTokenizer`` to be able to split words on a delimiter other than a space. We do this by
creating a ``PluginConfig`` that contains a property for the delimiter::

  @Plugin(type = "tokenizer")
  @Name("default")
  public class DefaultTokenizer implements Tokenizer {
    private final TokenizerConfig config;

    public static class TokenizerConfig extends PluginConfig {
      @Nullable
      private String delimiter;

      public TokenizerConfig() {
        this.delimiter = " ";
      }
    }

    public DefaultTokenizer(TokenizerConfig config) {
      this.config = config;
    }

    @Override
    public Iterable<String> tokenize(String line) {
      return Splitter.on(config.delimiter).split(line);
    }
  }

The ``@Nullable`` annotation tells CDAP that the field is not required. Without that annotation,
CDAP will complain if no plugin property for ``delimiter`` is given.
When we register the plugin, we need to pass in the properties that will be used to populate the
``PluginConfig`` passed to the ``DefaultTokenizer``. In this example, that means the ``delimiter``
property must be given when registering the plugin::

  public class WordCountApp extends AbstractApplication<WordCountApp.TokenizerConfig> {

    public static class TokenizerConfig extends Config {
      private String tokenizer;
      private Map<String, String> tokenizerProperties;
    }

    @Override
    public void configure() {
      TokenizerConfig config = getConfig();
      // usePlugin(type, name, id, properties)
      usePlugin("tokenizer", config.tokenizer, "tokenizerId", PluginProperties.builder()
        .addAll(config.tokenizerProperties).build());
      addMapReduce(new WordCount());
    }
  }

Now we can create an application that uses a comma instead of a space to split text::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/wordcount2 -H 'Content-Type: application/json' -d '{
    "artifact": { "name": "wordcount", "version": "1.2.0", "scope": "user" },
    "config": {
      "tokenizer": "default",
      "tokenizerProperties": { "delimiter": "," }
    }
  }'

.. rubric:: Third-Party Plugins

Sometimes there is a need to use a third-party JAR as a plugin. For example, you may want to be able to use
a JDBC driver as a plugin. In these situations, you have no control over the code, which means you cannot
annotate the relevant class with the ``@Plugin`` annotation. If this is the case, you can explicitly specify
the plugins using the ``Artifact-Plugins`` header when deploying the artifact::

  curl localhost:10000/v3/namespaces/default/artifacts/mysql-connector-java -H 'Artifact-Plugins: [ { "name": "mysql", "type": "jdbc", "className": "com.mysql.jdbc.Driver" } ]' --data-binary @mysql-connector-java-5.1.3.jar


.. rubric:: Learn More

More information about plugins can be found by looking at the ETL plugins included with CDAP.
You can also read the :ref:`Artifact HTTP RESTful API <http-restful-api-artifact>` for more information
on what plugin information is exposed through the API. 
