package com.continuuity.gradle

import org.gradle.api.Project
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Enables URLs that start with "classpath" to connect to resources
 * using the class loader.
 */
class ClasspathHandler extends URLStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(getClass());

    private final ClassLoader classLoader;

    public ClasspathHandler() {
        this.classLoader = getClass().getClassLoader();
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        String resourcePath = u.getHost() + u.getPath();
        logger.info("Loading resource: $resourcePath")
        final URL resourceUrl = classLoader.getResource(resourcePath);
        return resourceUrl.openConnection();
    }

    private static void register ()
    {
        try
        {
            // Enables URLs that start with "classpath" to be used so that resources can be loaded from jars.
            ConfigurableStreamHandlerFactory streamHandlerFactory = new ConfigurableStreamHandlerFactory("classpath",
                    new ClasspathHandler());
            URL.setURLStreamHandlerFactory(streamHandlerFactory);
        }
        catch (Throwable t)
        {
            logger.info("Skipping register of streamHandlerFactory: " + t.message);
        }
    }

    private static class ConfigurableStreamHandlerFactory implements URLStreamHandlerFactory {
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
