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

/*
 * For a detailed explanation regarding each configuration property and type check, visit:
 * https://jestjs.io/docs/en/configuration.html
 */

module.exports = {
  // automock: true,
  // clearMocks: true,
  globals: {
    'ts-jest': {
      babelConfig: {
        plugins: ['@babel/plugin-transform-modules-commonjs'],
      },
    },
  },
  moduleDirectories: ['node_modules', 'jest', __dirname],
  moduleNameMapper: {
    '\\.(jpg|jpeg|png|gif|eot|otf|webp|svg|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga)$':
      '<rootDir>/__mocks__/fileMock.js',
    '.scss$': '<rootDir>/__mocks__/styleMocks.js',
    '/^components/': '<rootDir>/app/cdap/components',
    '/^services/': '<rootDir>/app/cdap/services',
    '/^api/': '<rootDir>/app/cdap/api',
    '^lib': '<rootDir>/../lib',
    '^mocks': '<rootDir>/__mocks__',
  },
  modulePathIgnorePatterns: [
    './app/cdap/components/AbstractWidget/SchemaEditor/Context/__tests__/schemas.js',
    './app/cdap/components/AbstractWidget/SchemaEditor/Context/__tests__/data',
  ],
  modulePaths: ['./app/cdap/'],
  preset: 'ts-jest/presets/js-with-babel',
  roots: ['./app/cdap/'],
  setupFilesAfterEnv: ['./jest/jest.setup.js'],
  testEnvironment: 'jsdom',
  transform: {
    '.+\\.(css|styl|less|sass|scss)$': 'jest-css-modules-transform',
    '\\.[jt]sx?$': 'babel-jest',
  },
  transformIgnorePatterns: ['<rootDir>/node_modules/?!(@material)/'],
};
