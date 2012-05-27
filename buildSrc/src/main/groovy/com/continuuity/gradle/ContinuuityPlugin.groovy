package com.continuuity.gradle

import org.gradle.api.Project
import org.gradle.api.Plugin

/**
 * Created with IntelliJ IDEA.
 * User: Eric
 * Date: 5/26/12
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */
class ContinuuityPlugin implements Plugin<Project> {

    @Override
    void apply(Project t) {

        // Enables URLs that start with "classpath" to be used so that resources can be loaded from jars.
        ConfigurableStreamHandlerFactory streamHandlerFactory = new ConfigurableStreamHandlerFactory("classpath",
                new ClasspathHandler());
        URL.setURLStreamHandlerFactory(streamHandlerFactory);

        Map map = new HashMap();
        map.put("from", new URL("classpath:com/continuuity/gradle/common.gradle"));
        t.apply(map);

        map.put("from", new URL("classpath:com/continuuity/gradle/sonar.gradle"));
        t.apply(map);

        map.put("from", new URL("classpath:com/continuuity/gradle/clover.gradle"));
        t.apply(map);
    }

    /** A {@link URLStreamHandler} that handles resources on the classpath. */
    public class ClasspathHandler extends URLStreamHandler {
        /** The classloader to find resources from. */
        private final ClassLoader classLoader;

        public ClasspathHandler() {
            this.classLoader = getClass().getClassLoader();
        }

        @Override
        protected URLConnection openConnection(URL u) throws IOException {
            String resourcePath = u.getHost() + u.getPath();
            println "Loading resource: $resourcePath"
            final URL resourceUrl = classLoader.getResource(resourcePath);
            return resourceUrl.openConnection();
        }
    }

    class ConfigurableStreamHandlerFactory implements URLStreamHandlerFactory {
        private final Map<String, URLStreamHandler> protocolHandlers;

        public ConfigurableStreamHandlerFactory(String protocol, URLStreamHandler urlHandler) {
            protocolHandlers = new HashMap<String, URLStreamHandler>();
            addHandler(protocol, urlHandler);
        }

        public void addHandler(String protocol, URLStreamHandler urlHandler) {
            protocolHandlers.put(protocol, urlHandler);
        }

        public URLStreamHandler createURLStreamHandler(String protocol) {
            return protocolHandlers.get(protocol);
        }
    }
}
