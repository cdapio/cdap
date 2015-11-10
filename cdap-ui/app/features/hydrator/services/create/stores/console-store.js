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

class ConsoleStore {
  constructor(ConsoleDispatcher) {
    this.state = {};
    this.setDefaults();
    this.changeListeners = [];

    let dispatcher = ConsoleDispatcher.getDispatcher();
    dispatcher.register('onAddMessage', this.addMessage.bind(this));
    dispatcher.register('onResetMessages', this.resetMessages.bind(this));
  }

  setDefaults() {
    this.state = {
      messages: []
    };
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  getMessages() {
    return this.state.messages;
  }

  addMessage(message) {
    message.date = new Date();
    this.state.messages.push(message);
    this.emitChange();
  }

  resetMessages() {
    this.state.messages = [];
    this.emitChange();
  }


}

ConsoleStore.$inject = ['ConsoleDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConsoleStore', ConsoleStore);
