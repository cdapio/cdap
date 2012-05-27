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
    void apply(Project p) {

        String pluginName = getClass().getName();
        // Only apply if not already applied.
        if(!p.extensions.getExtraProperties().has(pluginName))
        {
            ClasspathHandler.register();
            if(!p.getSubprojects().isEmpty())
            {
                println ":continuuity plugin:multi-module"

                p.allprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/allprojects.gradle")
                }

                p.subprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/subprojects.gradle")
                }

                applyFrom(p, "classpath:com/continuuity/gradle/sonar.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")

                displayProjectInfo(p);
            }
            else
            {
                println ":continuuity plugin:standalone"

                applyFrom(p, "classpath:com/continuuity/gradle/allprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/subprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")

                displayProjectInfo(p);
            }

            p.allprojects
            {
                extensions.getExtraProperties().set(pluginName, 'true');
            }
        }
    }

    void displayProjectInfo (Project project)
    {
        println "-------------------------------------------------------------------------------------"
        println "PROJECT_NAME:    " + project.getProperties().get("artifactId");
        println "ROOT_DIR:        " + project.projectDir
        println "JAVA_VERSION:    " + System.properties.get("java.version") + " " +
                System.properties.get("java.version") + " " +
                System.properties.get("java.vendor") + " " +
                System.properties.get("os.name") + " " +
                System.properties.get("os.version") + " " +
                System.properties.get("os.arch");
        println "GRADLE_VERSION:  " + project.getGradle().getGradleVersion();
        println "VERSION:         " + project.getProperties().get("version");
        println "-------------------------------------------------------------------------------------"
    }

    void applyFrom (Project p, String uri)
    {
        Map map = new HashMap();
        map.put("from", new URL(uri));
        p.apply (map);
    }
}
