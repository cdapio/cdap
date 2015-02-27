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
    var d = new Date();
    var obj = {
      "pro1": {
        "pro11": 11,
        "pro12": "string",
        "pro13": true,
        "pro14": d,
        "pro15": null
      },
      "pro2": false,
      "pro3": "string else",
      "pro5": null
    };
    it('is a method', function() {
      expect(myHelpers.objectQuery).toEqual(jasmine.any(Function));
    });

    it('returns appropriate value if present: 1(number)', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro11")).toEqual(11);
    });

    it('returns appropriate value if present: 2(boolean)', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro13")).toEqual(true);
    });

    it('returns appropriate value if present: 3(string)', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro12")).toEqual("string");
    });

    it('returns appropriate value if present: 4(date)', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro14")).toEqual(d);
    });

    it('returns appropriate value if present: 5(object)', function() {
      expect(myHelpers.objectQuery(obj, "pro1")).toEqual({
        "pro11": 11,
        "pro12": "string",
        "pro13": true,
        "pro14": d,
        "pro15": null
      });
    });

    it('returns undefined if value not present: 1', function() {
      expect(myHelpers.objectQuery(obj, "something")).toEqual(undefined);
    });

    it('returns undefined if value not present: 2', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro14", "pro141")).toEqual(undefined);
    });

    it('returns null if explicitly set to null: 1', function() {
      expect(myHelpers.objectQuery(obj, "pro1", "pro15")).toEqual(null);
    });

    it('returns null if explicitly set to null: 2', function() {
      expect(myHelpers.objectQuery(obj, "pro5")).toEqual(null);
    });

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
