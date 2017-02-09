/*
 * Copyright © 2016 Cask Data, Inc.
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

 import T from 'i18n-react';
 T.setTexts(require('../cdap/text/text-en.yaml'));
 var Store = require('../cdap/services/NamespaceStore').default;
 var Header = require('../cdap/components/Header').default;
 var Wrangler = require('../wrangler/components/Wrangler').default;
 var globalEvents = require('../cdap/services/global-events').default;
 var ee = require('event-emitter');
 var VersionStore = require('../cdap/services/VersionStore').default;
 var VersionActions = require('../cdap/services/VersionStore/VersionActions').default;
 export {
  Store,
  Header,
  Wrangler,
  globalEvents,
  ee,
  VersionStore,
  VersionActions
};
