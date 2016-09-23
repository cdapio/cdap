.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _plugins:

=======
Plugins
=======

.. highlight:: java

A *Plugin* is a Java class that extends an application class by implementing an interface expected by the
application class. *Plugins* can be packaged in a separate artifact from the application class that uses it.

.. _plugins-usage:

Plugin Usage
============
You tell CDAP that a class is a *Plugin* by annotating the class with the type and name of the plugin.
For example::

  @Plugin(type = "runnable")
  @Name("noop")
  public class NoOpRunnable implements Runnable {

    public abstract void run() {
      // do nothing
    }
  }

A program can register a plugin at configure time (application creation time) by specifying the plugin type,
name, properties, and assigning an id to the plugin::

  public class ExampleWorker extends AbstractWorker {

    @Override
    public void configure() {
      usePlugin("runnable", "noop", "id", PluginProperties.builder().build());
    }
  }

Once registered, the plugin can be instantiated and used at runtime using the plugin id it was registered with::

  public class ExampleWorker extends AbstractWorker {
    private Runnable runnable;

    @Override
    public void configure() {
      usePlugin("runnable", "noop", "id", PluginProperties.builder().build());
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      runnable = context.newPluginInstance("id");
    }

    @Override
    public void run() {
      runnable.run();
    }
  }

.. _plugins-config:

Plugin Config
=============
A *Plugin* can also make use of the *PluginConfig* class to configure itself. Suppose we want
to modify our no-op runnable to print a configurable message. We can do this by adding a
*PluginConfig*, passing it into the constructor, and setting it as a field::

  @Plugin(type = "runnable")
  @Name("noop")
  public class NoOpRunnable implements Runnable {
    private final Conf conf;

    public static class Conf extends PluginConfig {
      @Nullable
      @Macro
      private String message;

      public Conf() {
        this.message = "Hello World!";
      }
    }

    public NoOpRunnable(Conf conf) {
      this.conf = conf;
    }

    public abstract void run() {
      System.out.println(conf.message);
    }
  }

Your extension to *PluginConfig* must contain only primitive, boxed primitive, or ``String`` types.
The *PluginConfig* passed in to the *Plugin* has its fields populated using the *PluginProperties*
specified when the *Plugin* was registered. In this example, if we want the message to be "Hello CDAP!"::

  public class ExampleWorker extends AbstractWorker {

    @Override
    public void configure() {
      usePlugin("runnable", "noop", "id", PluginProperties.builder()
        .add("message", "Hello CDAP!")
        .build());
    }
  }

- The ``@Nullable`` annotation tells CDAP that the field is not required. Without that annotation,
  CDAP will complain if no plugin property for ``delimiter`` is given. 
- Configuration fields can be annotated with an ``@Description`` that will be returned by the
  :ref:`Artifact HTTP RESTful API <http-restful-api-artifact-plugin-detail>` *Plugin Detail*.
- The ``@Macro`` annotation makes the field ``message`` *macro-enabled*; this allows the value of
  the field ``message`` to be a "macro key" whose value will be set at runtime.

.. _plugins-third-party:

.. highlight:: console

Third-Party Plugins
===================
Sometimes there is a need to use classes in a third-party JAR as plugins. For example, you may want to be able to use
a JDBC driver as a plugin. In these situations, you have no control over the code, which means you cannot
annotate the relevant class with the ``@Plugin`` annotation. If this is the case, you can explicitly specify
the plugins when deploying the artifact. For example, if you are using the HTTP RESTful API, you set the
``Artifact-Plugins``, ``Artifact-Version``, and ``Artifact-Extends`` headers when deploying the artifact:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/artifacts/mysql-connector-java" \
  -H 'Artifact-Plugins: [ { "name": "mysql", "type": "jdbc", "className": "com.mysql.jdbc.Driver" } ]' \
  -H "Artifact-Version: 5.1.35" \
  -H "Artifact-Extends: system:cdap-data-pipeline[|version|, |version|]/system:cdap-data-streams[|version|, |version|]" \
  --data-binary @mysql-connector-java-5.1.35.jar

Or, using the CDAP CLI:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
 
    |cdap >| load artifact /path/to/mysql-connector-java-5.1.35.jar config-file /path/to/config.json
    
    
where ``config.json`` contains:

.. highlight:: xml

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "system:cdap-data-pipeline\[|version|,\ |version|]", "system:cdap-data-streams[|version|,\ |version|]" ],
      "plugins": [
        {
          "name": "mysql",
          "type": "jdbc",
          "className": "com.mysql.jdbc.Driver"
        }
      ]
    }


.. _plugins-deployment:

Plugin Deployment
=================

.. _plugins-deployment-artifact:

To make plugins available to another artifact (and thus available to any application
created from one of the artifacts), the plugins must first be packaged in a JAR file.
After that, the JAR file must be deployed either as a :ref:`system artifact 
<plugins-deployment-system>` or a :ref:`user artifact <plugins-deployment-user>`.

A system artifact is available to users across any namespace. A user artifact is available
only to users in the namespace to which it is deployed. By design, deploying as a user
artifact just requires access to the :ref:`Artifact HTTP RESTful API <http-restful-api-artifact-add>`,
while deploying as a system artifact requires access to the filesystem of the CDAP Master.
This then requires administrator access and permission.

.. _plugins-deployment-packaging:

Plugin Packaging
----------------
A *Plugin* is packaged as a JAR file, which contains inside the plugin classes and their dependencies.
CDAP uses the "Export-Package" attribute in the JAR file manifest to determine
which classes are *visible*. A *visible* class is one that can be used by another class
that is not from the plugin JAR itself. This means the Java package which the plugin class
is in must be listed in "Export-Package", otherwise the plugin class will not be visible,
and hence no one will be able to use it. This can be done in Maven by editing your pom.xml.
For example, if your plugins are in the ``com.example.runnable`` and ``com.example.callable``
packages, you would edit the bundler plugin in your pom.xml:

.. code-block:: xml

  <plugin>
    <groupId>org.apache.felix</groupId>
    <artifactId>maven-bundle-plugin</artifactId>
    <version>2.3.7</version>
    <extensions>true</extensions>
    <configuration>
      <instructions>
        <Embed-Dependency>*;inline=false;scope=compile</Embed-Dependency>
        <Embed-Transitive>true</Embed-Transitive>
        <Embed-Directory>lib</Embed-Directory>
        <Export-Package>com.example.runnable;com.example.callable</Export-Package>
      </instructions>
    </configuration>
    ...
  </plugin>


.. _plugins-deployment-system:

Deploying as a System Artifact
------------------------------
To deploy the artifact as a system artifact, both the JAR file and a matching configuration file
must be placed in the appropriate directory.

- **Standalone mode:** ``$CDAP_INSTALL_DIR/artifacts``

- **Distributed mode:** The plugin JARs should be placed in the local file system and the path
  can be provided to CDAP by setting the property ``app.artifact.dir`` in
  :ref:`cdap-site.xml <appendix-cdap-site.xml>`. Multiple directories can be defined by separating
  them with a semicolon. The default path is ``/opt/cdap/master/artifacts``.

For each plugin JAR, there must also be a corresponding configuration file to specify which artifacts
can use the plugins. The file name must match the name of the JAR, except it must have the ``.json``
extension instead of the ``.jar`` extension. For example, if your JAR file is named
``custom-transforms-1.0.0.jar``, there must be a corresponding ``custom-transforms-1.0.0.json`` file.
If your ``custom-transforms-1.0.0.jar`` contains transforms that can be used by both the ``cdap-data-pipeline``
and ``cdap-data-streams`` artifacts, ``custom-transforms-1.0.0.json`` would contain:

.. highlight:: json

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "cdap-data-pipeline[|version|,\ |version|]", "cdap-data-streams[|version|,\ |version|]" ]
    }

This file specifies that the plugins in ``custom-transforms-1.0.0.jar`` can be used by version |version| of
the ``cdap-data-pipeline`` and ``cdap-data-streams`` artifacts. You can also specify a wider range of versions
that can use the plugins, with square brackets ``[ ]`` indicating an inclusive version and parentheses ``( )`` indicating
an exclusive version. For example:

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "cdap-data-pipeline[3.5.0,4.0.0)", "cdap-data-streams[3.5.0,4.0.0)" ]
    }

specifies that these plugins can be used by versions 3.5.0 (inclusive) to 4.0.0 (exclusive) of the
``cdap-data-pipeline`` and ``cdap-data-streams`` artifacts.

If the artifact contains third-party plugins, you can explicitly list them in the config file.
For example, you may want to deploy a JDBC driver contained in a third-party JAR. In these cases,
you have no control over the code to annotate the classes that should be plugins, so you need to
list them in the configuration:

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "cdap-data-pipeline[3.5.0,4.0.0)", "cdap-data-streams[3.5.0,4.0.0)" ],
      "plugins": [
        {
          "name": "mysql",
          "type": "jdbc",
          "className": "com.mysql.jdbc.Driver"
        }
      ]
    }

Once your JARs and matching configuration files are in place, a CDAP CLI command (``load artifact``) or 
a HTTP RESTful API call to :ref:`load system artifacts <http-restful-api-artifact-system-load>`
can be made to load the artifacts. As described in the documentation on :ref:`artifacts`, only
snapshot artifacts can be re-deployed without requiring that they first be deleted.

Alternatively, the CDAP Standalone should be restarted for this change to take effect in Standalone
mode, and ``cdap-master`` services should be restarted in the Distributed mode.

.. _plugins-deployment-user:

Deploying as a User Artifact
----------------------------
To deploy the artifact as a user artifact, use the :ref:`Artifact HTTP RESTful API 
<http-restful-api-artifact-add>` *Add Artifact* or the CLI. 

When using the HTTP RESTful API, you will need to specify the ``Artifact-Extends`` header.
Unless the artifact's version is defined in the manifest file of the JAR file you upload,
you will also need to specify the ``Artifact-Version`` header.

When using the CLI, a configuration file exactly like the one described in the
:ref:`Deploying as a System Artifact <plugins-deployment-system>` must be used.

For example, to deploy ``custom-transforms-1.0.0.jar`` using the RESTful API:

.. tabbed-parsed-literal::

    $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/artifacts/custom-transforms" \
    -H "Artifact-Extends: system:cdap-data-pipeline[|version|, |version|]/system:cdap-data-streams[|version|, |version|]" \
    --data-binary @/path/to/custom-transforms-1.0.0.jar

Using the CLI:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
 
    |cdap >| load artifact /path/to/custom-transforms-1.0.0.jar config-file /path/to/config.json

where ``config.json`` contains:

.. highlight:: json

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "system:cdap-data-pipeline[|version|,\ |version|]", "system:cdap-data-streams[|version|,\ |version|]" ]
    }

Note that when deploying a user artifact that extends a system artifact,
you must prefix the parent artifact name with ``'system:'``.
This is in the event there is a user artifact with the same name as the system artifact.
If you are extending a user artifact, no prefix is required.

You can deploy third-party JARs in the same way except the plugin information needs
:ref:`to be explicitly listed <plugins-third-party>`. As described in the documentation on
:ref:`artifacts`, only snapshot artifacts can be re-deployed without requiring that they
first be deleted.

Using the RESTful API (note that if the artifact version is not in the JAR manifest file,
it needs to be set explicitly, as the JAR contents are uploaded without the filename):

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/artifacts/mysql-connector-java" \
  -H 'Artifact-Plugins: [ { "name": "mysql", "type": "jdbc", "className": "com.mysql.jdbc.Driver" } ]' \
  -H "Artifact-Version: 5.1.35" \
  -H "Artifact-Extends: system:cdap-data-pipeline[|version|, |version|]/system:cdap-data-streams[|version|, |version|]" \
  --data-binary @mysql-connector-java-5.1.35.jar

Using the CLI (note that the artifact version, if not explicitly set, is derived from the JAR filename):

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
 
    |cdap >| load artifact /path/to/mysql-connector-java-5.1.35.jar config-file /path/to/config.json

where ``config.json`` contains:

.. highlight:: xml

.. container:: highlight

  .. parsed-literal:: 
    {
      "parents": [ "system:cdap-data-pipeline\[|version|,\ |version|]", "system:cdap-data-streams[|version|,\ |version|]" ],
      "plugins": [
        {
          "name": "mysql",
          "type": "jdbc",
          "className": "com.mysql.jdbc.Driver"
        }
      ]
    }

.. _plugins-deployment-verification:

Deployment Verification
-----------------------
You can verify that a plugin artifact was added successfully by using the
:ref:`Artifact HTTP RESTful API <http-restful-api-artifact-detail>` to retrieve artifact details.
For example, to retrieve detail about our ``custom-transforms`` artifact:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "localhost:10000/v3/namespaces/default/artifacts/custom-transforms/versions/1.0.0?scope=[system | user]

Using the CLI:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
 
    |cdap >| describe artifact properties custom-transforms 1.0.0 [system | user]
    
If you deployed the ``custom-transforms`` artifact as a system artifact, the scope is ``system``.
If you deployed the ``custom-transforms`` artifact as a user artifact, the scope is ``user``.

You can verify that the plugins in your newly-added artifact are available to its parent by using the
:ref:`Artifact HTTP RESTful API <http-restful-api-artifact-available-plugins>` to list plugins of a
specific type. For example, to check if ``cdap-data-pipeline`` can access the plugins in the
``custom-transforms`` artifact:

.. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "localhost:10000/v3/namespaces/default/artifacts/cdap-data-pipeline/versions/|version|/extensions/transform?scope=system"

Using the CLI:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
 
    |cdap >| list artifact plugins cdap-data-pipeline |version| transform system
    
You can then check the list returned to see if your transforms are in the list. Note that
the scope here refers to the scope of the parent artifact. In this example it is the ``system``
scope because ``cdap-data-pipeline`` is a system artifact. This is true even if you deployed
``custom-transforms`` as a user artifact because the parent is still a system artifact.

.. _plugins-use-case:

Example Use Case
================
When writing an application class, it is often useful to create interfaces or abstract classes that define
a logical contract in your code, but do not provide an implementation of that contract. This lets you plug in
different implementations to fit your needs.

.. rubric:: Classic WordCount Example

.. highlight:: java

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

.. highlight:: console

We package our code into a JAR file named ``wordcount-1.0.0.jar`` and add it to CDAP:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/artifacts/wordcount" --data-binary @wordcount-1.0.0.jar

We then create an application from that artifact:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "localhost:10000/v3/namespaces/default/apps/basicwordcount" -H "Content-Type: application/json" \
  -d '{ "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" } }'
    
This program runs just fine. It counts all words in the input. However, what if we want to count phrases
instead of words? Or what if we want to filter out common words such as ``'the'`` and ``'a'``? We would not want
to copy and paste our application class and then make just small tweaks.

.. rubric:: A Configurable Application

Instead, we would like to be able to create applications that
are configured to tokenize the line in different ways. That is, if we want an application that filters
stopwords, we want to be able to create it through a configuration:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "localhost:10000/v3/namespaces/default/apps/stopwordcount" -H "Content-Type: application/json" \
  -d '{ "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" }, "config": { "tokenizer": "stopword" } }'
  
Similarly, we want to be able to create an application that counts phrases through a configuration:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "localhost:10000/v3/namespaces/default/apps/phrasecount" -H "Content-Type: application/json" \
  -d '{ "artifact": { "name": "wordcount", "version": "1.0.0", "scope": "user" }, "config": { "tokenizer": "phrase" } }'

.. highlight:: java

This is possible by changing our code to use the *Plugin* framework. The first thing we need to do is
introduce a ``Tokenizer`` interface::

  public interface Tokenizer {
    Iterable<String> tokenize(String line);
  }

Now we change our ``WordCountMapper`` to use the plugin framework to instantiate and use a ``Tokenizer``::

  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    implements ProgramLifecycle<MapReduceTaskContext> {
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
    public void initialize(MapReduceTaskContext context) throws Exception {
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
If it receives this request:

.. code-block:: json

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
package in our pom.xml:

.. code-block:: xml

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

We then package the code in a new version of the artifact ``wordcount-1.1.0.jar`` and deploy it:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/artifacts/wordcount" --data-binary @wordcount-1.1.0.jar

.. rubric:: Implementing Tokenizer Plugins

Finally, we need to implement some tokenizer plugins. *Plugins* are just Java classes that have
been annotated with a plugin type and name:

.. code-block:: java

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

We package these tokenizers in a separate artifact named ``tokenizers-1.0.0.jar``. In order to make these
plugins visibile to programs using them, we need to include their packages in the ``Export-Packages``
manifest attribute. For example, if our classes are all in the ``com.example.tokenizer`` package,
we need to expose the ``com.example.tokenizer`` package in our pom.xml:

.. code-block:: xml

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
        <Export-Package>com.example.tokenizer</Export-Package>
      </instructions>
    </configuration>
    ...
  </plugin>

.. highlight:: console

When deploying this artifact, we tell CDAP that the artifact extends the ``wordcount`` artifact, versions
``1.1.0`` inclusive to ``2.0.0`` exclusive:

.. tabbed-parsed-literal::

 $ curl -w"\n" "localhost:10000/v3/namespaces/default/artifacts/tokenizers" --data-binary @tokenizers-1.0.0.jar \
 -H "Artifact-Extends:wordcount[1.1.0,2.0.0)"

This will make the plugins available to those versions of the ``wordcount`` artifact. We can now create
applications that use the tokenizer we want:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT localhost:10000/v3/namespaces/default/apps/phrasecount -H "Content-Type: application/json" \
  -d '{ "artifact": { "name": "wordcount", "version": "1.1.0", "scope": "user" }, "config": { "tokenizer": "phrase" } }'

.. rubric:: Adding a Plugin Configuration to the Application

.. highlight:: java

After a while, we find that we need to support reading files where words are delimited by a character
other than a space. We decide to modify our ``DefaultTokenizer`` to use a ``PluginConfig`` that contains
a property for the delimiter::

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

.. highlight:: console

Now we can create an application that uses a comma instead of a space to split text (re-formatted for display):

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "localhost:10000/v3/namespaces/default/apps/wordcount2" -H "Content-Type: application/json" \
    -d '{ 
      "artifact": { "name": "wordcount", "version": "1.2.0", "scope": "user" },
      "config": { "tokenizer": "default", "tokenizerProperties": { "delimiter": "," }
      }
    }'
