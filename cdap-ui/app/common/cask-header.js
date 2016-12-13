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
 var HeaderActions = require('../cdap/components/HeaderActions').default;
 var HeaderBrand = require('../cdap/components/HeaderBrand').default;
 var Store = require('../cdap/services/NamespaceStore').default;
 var Wrangler = require('../wrangler/components/Wrangler').default;
 export {Store, HeaderBrand, HeaderActions, Wrangler};
