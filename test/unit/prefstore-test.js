'use strict';

describe('myUiSettings', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var myUiSettings;
  beforeEach(inject(function($injector) {
    myUiSettings = $injector.get('myUiSettings');
  }));

  // actual testing follows

  it('has the expected endpoint', function() {
    expect(myUiSettings.endpoint).toEqual('/preferences/uisettings');
  });


  describe('get', function() {

    it('is a method', function() {
      expect(myUiSettings.get).toEqual(jasmine.any(Function));
    });

    it('returns a promise', function() {
      var result = myUiSettings.get('test');
      expect(result.then).toEqual(jasmine.any(Function));
    });

  });



});