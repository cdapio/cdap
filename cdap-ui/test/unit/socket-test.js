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

'use strict';

describe('mySocket', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // mock SockJS dependency
  var mocked;
  beforeEach(module(function ($provide) {
    var send = jasmine.createSpy("SockJS.send");
    mocked = {
      SockJS: function() {
        this.send = send;
        this.readyState = 1;
        return this;
      },
      SockJS_send: send
    };
    spyOn(mocked, 'SockJS').and.callThrough();
    $provide.value('SockJS', mocked.SockJS);
  }));

  // get ref to the service to test
  var mySocket;
  beforeEach(inject(function($injector) {
    mySocket = $injector.get('mySocket');
  }));

  // actual testing follows
  it('exposes some methods', function() {
    expect(mySocket.init).toEqual(jasmine.any(Function));
    expect(mySocket.send).toEqual(jasmine.any(Function));
    expect(mySocket.close).toEqual(jasmine.any(Function));
  });

  it('calling init makes new SockJS', function() {
    var c = mocked.SockJS.calls.count();
    mySocket.init();
    expect(mocked.SockJS.calls.count()).toEqual(c+1);
  });

  describe('calls SockJS.send', function () {

    beforeEach(function() {
      mocked.SockJS_send.calls.reset();
    });

    it('with expected args', function() {
      var obj = {foo:"bar"};
      mySocket.send(obj);
      expect(mocked.SockJS_send).toHaveBeenCalled();

      var arg = JSON.parse(mocked.SockJS_send.calls.mostRecent().args[0]);
      expect(arg).toEqual(jasmine.objectContaining(obj));
      expect(Object.keys(arg)).toEqual(['user', 'foo']);
    });

    describe('after unfolding _cdapPath resource obj key', function() {

      it('without method', function() {
        var obj = {resource:{_cdapPath:'/foo/bar'}};
        mySocket.send(obj);
        expect(mocked.SockJS_send).toHaveBeenCalled();
        var arg = JSON.parse(mocked.SockJS_send.calls.mostRecent().args[0]);

        expect(arg.resource.method).toEqual('GET');
        expect(arg.resource.url).toMatch(/\/v3\/foo\/bar$/);
        expect(arg.resource._cdapPath).toBeUndefined();
      });

      it('with method', function() {
        var obj = {resource:{_cdapPath:'/foo/bar', method: 'POST'}};
        mySocket.send(obj);
        expect(mocked.SockJS_send).toHaveBeenCalled();
        var arg = JSON.parse(mocked.SockJS_send.calls.mostRecent().args[0]);

        expect(arg.resource.method).toEqual('POST');
        expect(arg.resource.url).toMatch(/\/v3\/foo\/bar$/);
        expect(arg.resource._cdapPath).toBeUndefined();
      });

    });

  });




});