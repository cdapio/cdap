'use strict';


describe('test env', function(){

  it('is ready', function() {
    expect(jasmine).toBeDefined();
    expect(angular).toBeDefined();
    expect(module).toBe(angular.mock.module);
    expect(inject).toBe(angular.mock.inject);
  });

});


describe('directive tests', function() {

  it('moved to *-unit-test.js files inside each directive directory', function() {
    expect(true).toBeTruthy();
  });

});

