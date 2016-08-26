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

class HydratorPlusPlusConsoleTabService {
  constructor(HydratorPlusPlusConsoleStore, myAlertOnValium) {
    this.HydratorPlusPlusConsoleStore = HydratorPlusPlusConsoleStore;
    this.myAlertOnValium = myAlertOnValium;
    this.setMessages();
  }
  listen() {
    this.unsub = this.HydratorPlusPlusConsoleStore.registerOnChangeListener( this.setMessages.bind(this) );
  }
  unsubscribe() {
    this.unsub();
  }
  setMessages() {
    let messages = this.HydratorPlusPlusConsoleStore.getMessages();
    if (Array.isArray(messages) && !messages.length) {
      return;
    }
    let missingNodesList = [];
    let errorMessage = [];
    let successMessage = [];
    messages.forEach( message => {
      switch(message.type) {
        case 'NO-SINK-FOUND':
          missingNodesList.push('sinks(s)');
          break;
        case 'NO-SOURCE-FOUND':
          missingNodesList.push('source(s)');
          break;
        case 'STRAY-NODES':
          errorMessage.push(message.payload.nodes.map( node => node.plugin.label).join(', ') + ' - nodes missing connections');
          break;
        case 'INVALID-CONNECTIONS':
          errorMessage.push(message.payload.connections.join(', ') + ' - invalid connection');
          break;
        case 'success':
          successMessage.push(message.content);
          break;
        case 'error':
          errorMessage.push(message.content);
          break;
      }
    });
    if (missingNodesList.length === 2) {
      errorMessage.push(`Missing ${missingNodesList.join(', ')} or actions in the pipeline.`);
    } else if (missingNodesList.length) {
      errorMessage.push(`Missing ${missingNodesList.join(', ')} in the pipeline.`);
    }
    if (successMessage.length) {
      this.myAlertOnValium.show({
        type: 'success',
        content: successMessage.join(', ')
      });
    } else if (errorMessage.length) {
      this.myAlertOnValium.show({
        type: 'danger',
        templateUrl: '/assets/features/hydrator/templates/partial/error-template.html',
        templateScope: {
          content: errorMessage,
          currentIndex: 0,
          moveToNextIndex : function() {
            if(errorMessage.length > this.currentIndex) {
              this.currentIndex += 1;
            }
          },
          moveToPrevIndex : function() {
            if (this.currentIndex > 0) {
              this.currentIndex -= 1;
            }
          }
        }
      });
    }
  }
}

HydratorPlusPlusConsoleTabService.$inject = ['HydratorPlusPlusConsoleStore', 'myAlertOnValium'];
angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorPlusPlusConsoleTabService', HydratorPlusPlusConsoleTabService);
