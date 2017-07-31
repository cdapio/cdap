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

import Datasource from 'services/datasource';
import {apiCreator} from 'services/resource-helper';

let dataSrc = new Datasource();
const basepath = '/namespaces/:namespace/apps/yare/services/service/methods';
const rbbasepath = `${basepath}/rulebooks/:rulebookid`;

const  MyRulesEngineApi = {
  getRulebooks: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/rulebooks`),
  getRules: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/rules`),
  createRule: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/rules`),
  getRuleDetails: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/rules/:ruleid`),
  addRuleToRuleBook: apiCreator(dataSrc, 'PUT', 'REQUEST', `${rbbasepath}/rules/:ruleid`),
  removeRuleFromRuleBook: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${rbbasepath}/rules/:ruleid`),
  getRulesForRuleBook: apiCreator(dataSrc, 'GET', 'REQUEST', `${rbbasepath}/rules`),
  createRulebook: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/rulebooks`)
};

export default MyRulesEngineApi;
