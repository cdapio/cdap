'use strict';

describe('myHelpers', function(){

  // include the module
  beforeEach(module('cdap-ui.services'));

  // get ref to the service to test
  var myHelpers;
  beforeEach(inject(function($injector) {
    myHelpers = $injector.get('myHelpers');
  }));




  describe('objectQuery', function() {

    it('is a method', function() {
      expect(myHelpers.objectQuery).toEqual(jasmine.any(Function));
    });

    // TODO: add some tests for objectQuery
  });





  describe('deepSet', function() {

    it('is a method', function() {
      expect(myHelpers.deepSet).toEqual(jasmine.any(Function));
    });

    it('sets a value on the passed object', function() {
      var obj = {1:2};
      myHelpers.deepSet(obj, 'foo', 'bar');
      expect(obj).toEqual({1:2, foo:'bar'});
    });

    it('sets a deep value on the passed object', function() {
      var obj = {thing:1};
      myHelpers.deepSet(obj, 'foo.bar.bat', 'man');
      expect(obj).toEqual({thing:1, foo: {bar: {bat: 'man'}}});
    });

  });

});