/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

var webpack = require('webpack');
var path = require('path');
var mode = process.env.NODE_ENV || 'production';
var UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const processEnv = {
  'NODE_ENV': JSON.stringify(mode),
  '__DEVTOOLS__': false
};

const getWebpackOutputObj = (mode) => {
  var output = {
    path: path.join(__dirname, 'dll'),
    filename: 'dll.cdap.[name].js',
    library: 'cdap_[name]'
  };
  if (mode === 'development') {
    output.filename = 'dll.cdap.[name].development.js';
  }
  return output;
};

const getWebpackDLLPlugin = (mode) => {
  var manifestFileName = 'cdap-[name]-manifest.json';
  if (mode === 'development') {
    manifestFileName = 'cdap-[name]-development-manifest.json';
  }
  return new webpack.DllPlugin({
    path: path.join(__dirname, 'dll', manifestFileName),
    name: 'cdap_[name]',
    context: path.resolve(__dirname, 'dll')
  });
};

var plugins = [
  new webpack.DefinePlugin({
    'process.env':processEnv,
  }),
  getWebpackDLLPlugin(mode)
];

if (mode === 'production') {
  plugins.push(
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
  );
}

var webpackConfig = {
  mode,
  entry: {
    vendor: [
      'whatwg-fetch',
      'uuid',
      'sockjs-client',
      'fuse.js',
      'react-dropzone',
      'react-redux',
      'redux-thunk',
      'redux-undo',
      'moment',
      'react-router',
      'react-router-dom',
      'react-sparklines',
      'react-tether',
      'react-timeago',
      'js-file-download',
      'mousetrap',
      'd3',
      'react-datetime',
      'svg4everybody',
      'vega',
      'vega-lite',
      'vega-tooltip',
      'react-helmet',
      'react-popper'
    ]
  },
  output: getWebpackOutputObj(mode),
  plugins,
  stats: {
    chunks: false,
    chunkModules: false
  },
  resolve: {
    modules: ['node_modules']
  }
};

module.exports = webpackConfig;
