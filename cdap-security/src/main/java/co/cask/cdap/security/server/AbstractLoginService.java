/*
 * Copyright © 2014-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.security.server;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public abstract class AbstractLoginService extends AbstractLifeCycle implements LoginService {

    private static final Logger LOG = Log.getLogger(AbstractLoginService.class);
    protected CConfiguration cConfiguration;
    protected String username_caseconversion ="";
    protected char[] username_CamelCaseConversionDelimiterArr ={'.'};

    protected AbstractLoginService(){
        cConfiguration = CConfiguration.create();

        String username_case_coversion = cConfiguration.get(Constants.Security.SECURITY_AUTH_USERNAME_CASECENVERSION_TYPE);
        if (username_case_coversion!=null) username_caseconversion = username_case_coversion.toLowerCase();

        String username_camelcase_coversion_delimiter = cConfiguration.get(Constants.Security.SECURITY_AUTH_USERNAME_CAMELCASE_DELIMITER);
        if (username_camelcase_coversion_delimiter!=null && username_camelcase_coversion_delimiter.length()>0) username_CamelCaseConversionDelimiterArr = username_camelcase_coversion_delimiter.toCharArray();

        LOG.debug("username_case_coversion: {}, username_camelcase_coversion_delimiter: {}", username_case_coversion, username_camelcase_coversion_delimiter);
    }

    public String usernameCaseConvert(String username){
        if(username==null || username_caseconversion.equalsIgnoreCase("")){
            return username;
        }
        switch(username_caseconversion.toLowerCase()){
            case "lower":
                username = username.toLowerCase();
                break;
            case "upper":
                username = username.toUpperCase();
                break;
            case "camelcase":
                username = org.apache.commons.lang3.text.WordUtils.capitalizeFully(username, username_CamelCaseConversionDelimiterArr);
                break;
            case "server":
                username = serverCaseConvert(username);
                break;
            default:
                LOG.debug("no match for type: " + username);
        }

        return username;
    }

    public abstract String serverCaseConvert(String username);
}
