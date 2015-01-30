'use strict';

describe('myHelpers', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var myHelpers;
  beforeEach(inject(function($injector) {
    myHelpers = $injector.get('myHelpers');
  }));

  // actual testing follows

  describe('objectQuery', function() {

    it('is a method', function() {
      expect(mySettings.objectQuery).toEqual(jasmine.any(Function));
    });

    // FIXME: add tests for objectQuery
  });


  describe('deepSet', function() {

    it('is a method', function() {
      expect(mySettings.deepSet).toEqual(jasmine.any(Function));
    });


  });

});