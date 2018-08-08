/*
 * Copyright © 2017 Cask Data, Inc.
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

var webpack = require('webpack');
var UglifyJsPlugin = require('uglifyjs-webpack-plugin');
var webpackConfig = {
  entry: './index.js',
  output: {
    filename: './upgrade.js',
    path: __dirname + '/dist'
  },
  module: {
    loaders: [
      {
        test: /\.js$/,
        use: ['babel-loader']
      }
    ]
  },
  plugins: [
    new UglifyJsPlugin({
      uglifyOptions: {
        ie8: false,
        compress: {
          warnings: false
        },
        output: {
          comments: false,
          beautify: false,
        }
      }
    })
  ],
  target: 'node'
};

module.exports = webpackConfig;

