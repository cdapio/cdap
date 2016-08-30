/*
 * Copyright © 2015 Cask Data, Inc.
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

module.exports = {
  resetStandaloneWelcomeMessage() {
    var config = require('./ui-settings.json');
    var fs = require('fs');
    var log = require('log4js').getLogger('default');
    var defer = require('q').defer();
    var path = require('path');
    config['standalone.welcome.message'] = 'false';
    try {
      fs.writeFileSync(path.join(__dirname, 'ui-settings.json'), JSON.stringify(config, null, 2));
      defer.resolve();
      return defer.promise;
    } catch(e) {
      log.error('Failure to reset Standalone Welcome message: ' + e);
      defer.reject(e);
      return defer.promise;
    }
  }
};
