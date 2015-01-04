'use strict';

describe('myUiPrefs', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var myUiPrefs;
  beforeEach(inject(function($injector) {
    myUiPrefs = $injector.get('myUiPrefs');
  }));

  // actual testing follows

  it('has the expected endpoint', function() {
    expect(myUiPrefs.endpoint).toEqual('/preferences/uisettings');
  });


  describe('get', function() {

    it('is a method', function() {
      expect(myUiPrefs.get).toEqual(jasmine.any(Function));
    });

    it('returns a promise', function() {
      var result = myUiPrefs.get('test');
      expect(result.then).toEqual(jasmine.any(Function));
    });

  });



});