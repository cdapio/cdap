/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import { parse, serialize } from 'components/AbstractWidget/FunctionDropdownArgumentsWidget/parser';

describe('FunctionDropdownArgumentsRow', () => {
  describe('parser', () => {
    describe('parse', () => {
      it('should parse a string with no arguments', () => {
        expect(parse('myAlias:myFunc(field1,true)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: true,
          arguments: '',
        });
      });

      it('should parse a string with ignoreNulls false', () => {
        expect(parse('myAlias:myFunc(field1,false)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: false,
          arguments: '',
        });
      });

      it('should parse a string with one argument', () => {
        expect(parse('myAlias:myFunc(field1,a,true)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: true,
          arguments: 'a',
        });
      });

      it('should parse a string with multiple arguments', () => {
        expect(parse('myAlias:myFunc(field1,a,b,c,true)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: true,
          arguments: 'a,b,c',
        });
      });

      it('should parse an argument with a colon', () => {
        expect(parse('myAlias:myFunc(field1,a:b,c,true)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: true,
          arguments: 'a:b,c',
        });
      });

      it('should treat an invalid ignoreNulls value as the default (true)', () => {
        expect(parse('myAlias:myFunc(field1,a,b,c,hjg)')).toEqual({
          alias: 'myAlias',
          func: 'myFunc',
          field: 'field1',
          ignoreNulls: true,
          arguments: 'a,b,c',
        });
      });

      it('should handle a missing closing parentheses', () => {
        expect(parse('myAlias:myFunc(field1,a,b,c,true')).toEqual({
          alias: 'myAlias',
          func: '',
          field: '',
          ignoreNulls: true,
          arguments: '',
        });
      });

      it('should handle a missing opening parentheses', () => {
        expect(parse('myAlias:myFuncfield1,a,b,c,true)')).toEqual({
          alias: 'myAlias',
          func: '',
          field: '',
          ignoreNulls: true,
          arguments: '',
        });
      });
    });

    describe('serialize', () => {
      it('should serialize inputs with no arguments', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: true,
            arguments: '',
          })
        ).toBe('myAlias:myFunc(field1,true)');
      });

      it('should handle spaces-only argument strings', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: true,
            arguments: '     ',
          })
        ).toBe('myAlias:myFunc(field1,true)');
      });

      it('should serialize inputs with one argument', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: true,
            arguments: 'a',
          })
        ).toBe('myAlias:myFunc(field1,a,true)');
      });

      it('should serialize inputs with multiple arguments', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: true,
            arguments: 'a,b,c',
          })
        ).toBe('myAlias:myFunc(field1,a,b,c,true)');
      });

      it('should handle arguments with leading an trailing spaces', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: true,
            arguments: '  a,b,c  ',
          })
        ).toBe('myAlias:myFunc(field1,a,b,c,true)');
      });

      it('should serialize false ignoreNulls', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: false,
            arguments: 'a,b,c',
          })
        ).toBe('myAlias:myFunc(field1,a,b,c,false)');
      });

      it('should return an empty string if the alias is not provided', () => {
        expect(
          serialize({
            alias: '',
            func: 'myFunc',
            field: 'field1',
            ignoreNulls: false,
            arguments: 'a,b,c',
          })
        ).toBe('');
      });

      it('should return an empty string if the func is not provided', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: '',
            field: 'field1',
            ignoreNulls: false,
            arguments: 'a,b,c',
          })
        ).toBe('');
      });

      it('should return an empty string if the field is not provided', () => {
        expect(
          serialize({
            alias: 'myAlias',
            func: 'myFunc',
            field: '',
            ignoreNulls: false,
            arguments: 'a,b,c',
          })
        ).toBe('');
      });
    });
  });
});
