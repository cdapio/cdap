/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;

/**
 *
 */
public class Hello {
  private static final Method obtainToken;
  private static final Method obtainAndCacheToken;

  static {
    try {
      Class c = Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil");
      obtainToken = c.getMethod("obtainToken", Configuration.class);
      obtainAndCacheToken = c.getMethod("obtainAndCacheToken", Configuration.class, UserGroupInformation.class);

    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public static Credentials obtainToken(Configuration hConf, Credentials credentials) throws Exception {
    Token<? extends TokenIdentifier> token = castToken(obtainToken.invoke(null, hConf));

    System.out.println("Fetched HBase delegation token:" +  token);

    return credentials;
  }

  public static UserGroupInformation obtainToken2(Configuration hConf, UserGroupInformation ugi) throws Exception {
    castToken(obtainAndCacheToken.invoke(null, hConf, ugi));

    System.out.println("Added HBase delegation token to ugi:" + ugi);
    System.out.println(ugi.getCredentials().getAllTokens().size());

    Token<? extends TokenIdentifier> token = ugi.getCredentials().getAllTokens().iterator().next();
    TokenIdentifier id = token.decodeIdentifier();
    System.out.println(id.getClass());
    System.out.println(id);
    System.out.println(new Gson().toJson(id));

    return ugi;
  }

  private static <T extends TokenIdentifier> Token<T> castToken(Object obj) {
    return (Token<T>) obj;
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();

    UserGroupInformation aliProxy = UserGroupInformation.createProxyUser("ali", UserGroupInformation.getLoginUser());

    if (args.length == 1) {

      obtainToken2(conf, aliProxy);

    } else {

      aliProxy.doAs(new PrivilegedExceptionAction<Credentials>() {
          @Override
          public Credentials run() throws Exception {
            return obtainToken(conf, new Credentials());
          }
      });

    }
  }
}


