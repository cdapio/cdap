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
                println "Applying Continuuity Plugin (Multi-module)"

                p.allprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/allprojects.gradle")
                }

                p.subprojects {
                    applyFrom(getProject(), "classpath:com/continuuity/gradle/subprojects.gradle")
                }

                applyFrom(p, "classpath:com/continuuity/gradle/sonar.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")
            }
            else
            {
                println "Applying Continuutiy Plugin (Standalone)"

                applyFrom(p, "classpath:com/continuuity/gradle/allprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/subprojects.gradle")
                applyFrom(p, "classpath:com/continuuity/gradle/clover.gradle")
            }

            p.allprojects
            {
                extensions.getExtraProperties().set(pluginName, 'true');
            }
        }
    }

    void applyFrom (Project p, String uri)
    {
        Map map = new HashMap();
        map.put("from", new URL(uri));
        p.apply (map);
    }
}
