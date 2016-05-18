// /*
//  * Copyright © 2015 Cask Data, Inc.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  * use this file except in compliance with the License. You may obtain a copy of
//  * the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  * License for the specific language governing permissions and limitations under
//  * the License.
//  */
//
// 'use strict';
//
// describe('mySettings', function(){
//
//   // include the module
//   beforeEach(module('cdap-ui.services'));
//
//   // mock SockJS dependency
//   var mocked;
//   beforeEach(module(function ($provide) {
//     var send = jasmine.createSpy("SockJS.send");
//     mocked = {
//       SockJS: function() {
//         this.send = send;
//         this.readyState = 1;
//         return this;
//       },
//       SockJS_send: send
//     };
//     spyOn(mocked, 'SockJS').and.callThrough();
//     $provide.value('SockJS', mocked.SockJS);
//   }));
//
//   // get ref to the service to test
//   var mySettings;
//   beforeEach(inject(function($injector) {
//     mySettings = $injector.get('mySettings');
//   }));
//
//
//   // actual testing follows
//   it('has the expected endpoint', function() {
//     expect(mySettings.endpoint).toEqual('/configuration/consolesettings');
//   });
//
//
//   describe('get', function() {
//
//     it('is a method', function() {
//       expect(mySettings.get).toEqual(jasmine.any(Function));
//     });
//
//     it('returns a promise', function() {
//       var result = mySettings.get('test');
//       expect(result.then).toEqual(jasmine.any(Function));
//     });
//
//   });
//
//
//
// });
