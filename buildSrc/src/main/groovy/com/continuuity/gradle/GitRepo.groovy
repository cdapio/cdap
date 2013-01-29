package com.continuuity.gradle

/**
 *  Provides git functionality from Gradle.
 */
class GitRepo {

    private static File baseDir;

    /**
     * The name of the repo.
     */
    private String name;

    /**
     * The branch the repo should point to.
     */
    private String branch;

    /**
     * The URL where the repo was cloned from.
     */
    private String origin;

    /**
     * The directory location of the repo.
     */
    private String dir;

    /**
     * The shell to use to run git commands.
     */
    private String shell;

    /**
     * The options to pass to the shell.
     */
    private String shellOption;

    /**
     * @return  the directory path associated with the repo.
     */
    String getDirectory ()
    {
        init();
        return dir;
    }

    /**
     * Clones the repo if it doesn' already exist.
     */
    void load ()
    {
        init();
        File destination = new File(baseDir, dir);
        if(!destination.exists())
        {
            String cmd = "git clone -vv --branch master $origin $destination.canonicalPath";
            println "$name: $cmd";
            runCommand(cmd);
        }
        if (branch == null || branch.isEmpty()) {
            String cmd = "git rev-parse --abbrev-ref HEAD"
            println "$name: $cmd"
            branch = runCommand(cmd, destination);
        }
    }

    /**
     * Checks out a branch.
     */
    void checkout ()
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git checkout ${branch}";
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Pulls from a repo.
     */
    void pull ()
    {
        init();
        File destination = new File(baseDir, dir);
        File gitDir = new File(destination, ".git");
        if(gitDir.exists())
        {
            String cmd = "git pull origin $branch";
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Runs status on a repo.
     */
    void status ()
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git status";
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Tags a repo locally
     */
    void tag (String tag)
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git tag " + tag;
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Pushes a local tag to remote 
     */
    void pushTag (String tag)
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git push origin " + tag;
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Creates a branch from head locally
     */
    void branch (String branchName)
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git checkout -b " + branchName;
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }

    /**
     * Pushes a local branch to remote 
     */
    void pushBranch (String branchName)
    {
        init();
        File destination = new File(baseDir, dir);
        if(destination.exists())
        {
            String cmd = "git push origin " + branchName;
            println "$name: $cmd";
            runCommand(cmd, destination);
        }
    }


    /**
     * Runs a shell command.
     * @param cmd   the command to run.
     * @param dir   the working directory to use.
     */
    private String runCommand (String cmd, File dir = null)
    {
        ProcessBuilder processBuilder = new ProcessBuilder(shell, shellOption, cmd)
        if(dir != null)
        {
            processBuilder.directory(dir)
        }
        processBuilder.redirectErrorStream(true);
        Process p = processBuilder.start();
        BufferedReader cmdOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String errline = ""
        StringBuilder outputBuilder = new StringBuilder();
        String separator = "";
        while((errline = cmdOut.readLine()) != null)
        {
            println errline;
            outputBuilder.append(errline).append(separator);
            separator = "\n";
        }
        if(p.waitFor() != 0)
        {
            println "Command failed: $cmd";
            BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = null;
            while ((line = stderr.readLine()) != null) {
                println line;
            }
            throw new Exception("Command failed: $cmd");
        }
        return outputBuilder.toString();
    }

    /**
     * Initializes defaults for options.
     */
    private void init ()
    {
        if(name == null)
        {
            throw new IllegalArgumentException("repoName is a required.")
        }

        if(branch == null)
        {
            branch = "master"
        }

        if(origin == null)
        {
            origin = String.format("git@github.com:continuuity/%s.git", name);
        }

        if(dir == null)
        {
            dir = name;
        }

        if(baseDir == null)
        {
            baseDir == new File(System.getProperty("user.dir"));
        }

        if (shell == null)
        {
            if (System.properties['os.name'].toLowerCase().contains('windows'))
            {
                shell = "cmd.exe"
                shellOption = "/c"
            }
            else
            {
                shell = "/bin/bash"
                shellOption = "-c"
            }
        }
    }
}
