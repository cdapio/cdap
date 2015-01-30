'use strict';

describe('mySettings', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var mySettings;
  beforeEach(inject(function($injector) {
    mySettings = $injector.get('mySettings');
  }));

  // actual testing follows

  it('has the expected endpoint', function() {
    expect(mySettings.endpoint).toEqual('/configuration/consolesettings');
  });


  describe('get', function() {

    it('is a method', function() {
      expect(mySettings.get).toEqual(jasmine.any(Function));
    });

    it('returns a promise', function() {
      var result = mySettings.get('test');
      expect(result.then).toEqual(jasmine.any(Function));
    });

  });



});