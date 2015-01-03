'use strict';

describe('myPrefStore', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var myPrefStore;
  beforeEach(inject(function($injector) {
    myPrefStore = $injector.get('myPrefStore');
  }));

  // actual testing follows

  describe('getPref', function() {

    it('is a method', function() {
      expect(myPrefStore.getPref).toEqual(jasmine.any(Function));
    });

    it('returns a promise', function() {
      var result = myPrefStore.getPref('test');
      expect(result.then).toEqual(jasmine.any(Function));
    });

  });



});