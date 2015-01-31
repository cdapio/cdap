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


  describe('deepGet', function() {

    it('is a method', function() {
      expect(myHelpers.deepGet).toEqual(jasmine.any(Function));
    });

    it('gets a value from the passed object', function() {
      var obj = {1:2, foo:'bar', baz: {bat: 'man'}};

      expect(myHelpers.deepGet(obj, 'foo'))
        .toEqual('bar');

      expect(myHelpers.deepGet(obj, '1'))
        .toEqual(2);

      expect(myHelpers.deepGet(obj, 'boom'))
        .toBeFalsy();
    });

    it('gets a deep value from the passed object', function() {
      var obj = {foo:{bar:{baz: {bat: 'man'}}}};

      expect(myHelpers.deepGet(obj, 'foo.bar.baz'))
        .toEqual({bat:'man'});
    });

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

    it('supports numbers as strings in the key', function() {
      var obj = {1:2};
      myHelpers.deepSet(obj, '3.4', 'aaa');
      expect(obj).toEqual({ 1:2, '3':{'4':'aaa'} });
    });

  });

});