/*
 * Copyright © 2017 Cask Data, Inc.
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


/**
 *  Should be able to refactor other places to use this shared function instead
 **/
export function constructCdapUrl(resource) {
  let url;

  // further sugar for building absolute url
  if (resource._cdapPath) {
    url = [
      window.CDAP_CONFIG.sslEnabled ? 'https://' : 'http://',
      window.CDAP_CONFIG.cdap.routerServerUrl,
      ':',
      window.CDAP_CONFIG.sslEnabled ? window.CDAP_CONFIG.cdap.routerSSLServerPort : window.CDAP_CONFIG.cdap.routerServerPort,
      '/v3',
      resource._cdapPath
    ].join('');

    delete resource._cdapPath;
  }

  return url;
}
