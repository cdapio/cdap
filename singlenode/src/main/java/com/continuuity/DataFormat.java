package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.DirUtils;

import java.io.*;

public class DataFormat {

    /**
     * This is our universal configurations object.
     */
    private static CConfiguration myConfiguration;

    /**
     * The name of the configuration governing where data-fabric data is stored
     */
    private static String dataDirPropName = "data.local.jdbc";

    /**
     *
     * command-line prompt to confirm user intentions
     *
     * @param prompt - String to display to user
     * @return - String input from user
     */
    private static String promptUser(String prompt) {
        System.out.print(prompt);

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String userinput = null;
        try {
            userinput = br.readLine().toLowerCase().trim();
        }  catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return userinput;
    }

    /**
     *
     * Main method
     *
      * @param args
     */
    public static void main(String[] args) {
        // first confirm user intends to delete all data
        String userinput = promptUser("Warning: this will delete any existing Continuuity user data.  Proceed? (yes/no): ");
        if(!("yes".equals(userinput) || "no".equals(userinput))) {
            // try once more
            userinput = promptUser("please type 'yes' or 'no': " );
        }
        if(!("yes".equals(userinput))) {
            System.out.println("Aborting.");
            System.exit(1);
        }

        // fetch configuration
        myConfiguration = CConfiguration.create();
        myConfiguration.clear();

        // TODO: Make this generic and scan for files before adding them
        myConfiguration.addResource("continuuity-flow.xml");
        myConfiguration.addResource("continuuity-gateway.xml");
        myConfiguration.addResource("continuuity-webapp.xml");
        myConfiguration.addResource("continuuity-data-fabric.xml");

        // read the relevant property
        String datadirjdbc = myConfiguration.get(dataDirPropName);
        // extract the relative file path from the jdbc string
        String datadirpath = datadirjdbc.replaceAll(".*file:", "");


        //String datadirpath = myConfiguration.get(dataDirPropName);
        if(datadirpath == null) {
            System.out.println("Error: cannot read data-fabric configuration. Aborting...");
            System.exit(-1);
        }

        // check if directory exists and process the delete
        File datadir = new File(datadirpath);
        if(!datadir.exists()) {
            System.out.println("Error: No existing data found.");
            System.exit(-1);
        } else {
            try {
                //System.out.println("deleting contents of " + datadir.toString());
                DirUtils.deleteDirectoryContents(datadir);
                System.out.println("Success");
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
