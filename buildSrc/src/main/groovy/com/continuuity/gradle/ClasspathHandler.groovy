package com.continuuity.gradle

import org.gradle.api.Project

/**
 * Created with IntelliJ IDEA.
 * User: Eric
 * Date: 5/27/12
 * Time: 9:42 AM
 * To change this template use File | Settings | File Templates.
 */
class ClasspathHandler extends URLStreamHandler {

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
            t.printStackTrace();
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
