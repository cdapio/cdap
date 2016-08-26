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

class HydratorPlusPlusConsoleActions {
  constructor(HydratorPlusPlusConsoleDispatcher) {
    this.hydratorPlusPlusConsoleDispatcher = HydratorPlusPlusConsoleDispatcher.getDispatcher();
  }

  addMessage(message) {
    this.hydratorPlusPlusConsoleDispatcher.dispatch('onAddMessage', message);
  }

  resetMessages() {
    this.hydratorPlusPlusConsoleDispatcher.dispatch('onResetMessages');
  }

}
HydratorPlusPlusConsoleActions.$inject = ['HydratorPlusPlusConsoleDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('HydratorPlusPlusConsoleActions', HydratorPlusPlusConsoleActions);
