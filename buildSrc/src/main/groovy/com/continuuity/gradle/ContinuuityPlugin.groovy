package com.continuuity.gradle

import org.gradle.api.Project
import org.gradle.api.Plugin

/**
 * Main class for continuuity plugin.
 */
class ContinuuityPlugin implements Plugin<Project> {

    @Override
    void apply(Project p) {

        String pluginName = getClass().getName();
        // Only apply if not already applied.
        if(!p.extensions.getExtraProperties().has(pluginName))
        {
            /// Register the classpath protocol handler.
            ClasspathHandler.register();

            // Check if a mult-module build or standalone build.
            if(!p.getSubprojects().isEmpty())
            {
                // multi-module build.
                println ":continuuity plugin:multi-module"

                /// Apply the subproject.gradle file to all subprojects (this is done first).
                p.subprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/subprojects.gradle")
                }

                /// Apply the allproject.gradle file to all projects.
                p.allprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/allprojects.gradle")
                }

                /// Apply sonar and clover.
                applyFrom(p, "classpath:com/continuuity/gradle/sonar.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")

                /// Load git repos information from setting.gradle file.
                p.apply(["from":"settings.gradle"])

                displayProjectInfo(p);
            }
            else
            {
                // standalone build.
                println ":continuuity plugin:standalone"

                /// Apply the allprojects, clover, and subprojects settings directly to the standalone project.
                applyFrom(p, "classpath:com/continuuity/gradle/subprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/allprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")

                displayProjectInfo(p);
            }

            /// Flag the plugin as already loaded.
            p.allprojects
            {
                extensions.getExtraProperties().set(pluginName, 'true');
            }
        }
    }

    /**
     * Displays useful project information to the console.
     * @param project the project being built.
     */
    void displayProjectInfo (Project project)
    {
        String projectName = project.getProperties().get("projectName");
        if((projectName == null) || (projectName.isEmpty()))
        {
            projectName = project.getProperties().get("artifactId");
        }
        println "-------------------------------------------------------------------------------------"
        println "PROJECT_NAME:    " + projectName
        println "ROOT_DIR:        " + project.projectDir
        println "JAVA_VERSION:    " + System.properties.get("java.version") + " " +
                System.properties.get("java.vendor") + " " +
                System.properties.get("os.name") + " " +
                System.properties.get("os.version") + " " +
                System.properties.get("os.arch");
        println "GRADLE_VERSION:  " + project.getGradle().getGradleVersion();
        println "VERSION:         " + project.getProperties().get("version");
        println "-------------------------------------------------------------------------------------"
    }

    /**
     * Applies gradle files to the specified project.
     * @param p   the project to apply the gradle project to.
     * @param uri  the location of the gradle file.
     */
    void applyFrom (Project p, String uri)
    {
        Map map = new HashMap();
        map.put("from", new URL(uri));
        p.apply (map);
    }
}
