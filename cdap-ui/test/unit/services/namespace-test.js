'use strict';

describe('myNamespace', function() {

  // include the module
  beforeEach(module('cdap-ui.services'));

  // mock MyDataSource dependency
  var mocked;
  var $rootScope, myNamespace;
  var requestSpy;

  beforeEach(module(function ($provide) {
    requestSpy = jasmine.createSpy('MyDataSource.request');

    mocked = {
      MyDataSource: function() {
        this.request = function() {
          return [
            { id: '1', name: 'namespace 1' },
            { id: '2', name: 'namespace 2' }
          ];
        };
        return this;
      }
    };

    //spyOn(mocked, 'MyDataSource').and.callThrough();
    $provide.value('MyDataSource', mocked.MyDataSource);
  }));

  beforeEach(inject(function(_$rootScope_, _myNamespace_) {
    $rootScope = _$rootScope_;
    myNamespace = _myNamespace_;
  }));

  // actual testing
  it('exposes some methods', function() {
    expect(myNamespace.getList).toEqual(jasmine.any(Function));
    expect(myNamespace.getDisplayName).toEqual(jasmine.any(Function));
  });


  // it('should return a promise when getList is called', function() {

  //   //var namespaceService = myNamespace($q, mocked.MyDataSource);
  //   // define the promise resolution method before calling "getList"
  //   var result;

  //   var promise = myNamespace.getList(true).then(function(res) {
  //     console.log('Executing the then of the promise.');
  //     result = res;
  //   });

  //   expect(result).toBeUndefined();

  //   $rootScope.$digest();
  //   //expect(result).toBeDefined();
  //   expect(requestSpy).toHaveBeenCalled();
  // });

});