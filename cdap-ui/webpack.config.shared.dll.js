/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
var TerserPlugin = require('terser-webpack-plugin');
var mode = process.env.NODE_ENV || 'production';
const isModeProduction = (mode) => mode === 'production' || mode === 'non-optimized-production';

const processEnv = {
  NODE_ENV: JSON.stringify(isModeProduction(mode) ? 'production' : 'development'),
  __DEVTOOLS__: false,
};

const getWebpackOutputObj = (mode) => {
  var output = {
    path: path.join(__dirname, 'packaged', 'public', 'dll'),
    filename: 'dll.shared.[name].js',
    library: 'shared_[name]',
    globalObject: 'window',
  };
  if (mode === 'development') {
    output.filename = 'dll.shared.[name].development.js';
  }
  return output;
};

const getWebpackDLLPlugin = (mode) => {
  var manifestFileName = 'shared-[name]-manifest.json';
  if (mode === 'development') {
    manifestFileName = 'shared-[name]-development-manifest.json';
  }
  return new webpack.DllPlugin({
    path: path.join(__dirname, 'packaged', 'public', 'dll', manifestFileName),
    name: 'shared_[name]',
    context: path.resolve(__dirname, 'packaged', 'public', 'dll'),
  });
};
var plugins = [
  new webpack.DefinePlugin({
    'process.env': processEnv,
  }),
  getWebpackDLLPlugin(mode),
];

var webpackConfig = {
  mode,
  entry: {
    vendor: [
      'react',
      'react-dom',
      'redux',
      'lodash',
      'classnames',
      'reactstrap',
      'i18n-react',
      'universal-cookie',
      'whatwg-fetch',
      'react-vis',
      'clipboard',
      'react-dnd-html5-backend',
      'react-dnd',
      'event-emitter',
      'react-loadable',
      'cdap-avsc',
      'css-vars-ponyfill',
    ],
  },
  optimization: {
    splitChunks: false,
  },
  output: getWebpackOutputObj(mode),
  stats: {
    assets: false,
    children: false,
    chunkGroups: false,
    chunkModules: false,
    chunkOrigins: false,
    chunks: false,
    modules: false,
  },
  plugins,
  resolve: {
    modules: ['node_modules'],
  },
};

if (isModeProduction(mode)) {
  webpackConfig.optimization.minimizer = [
    new TerserPlugin({
      terserOptions: {
        cache: false,
        parallel: true,
        sourceMap: true,
        extractComments: true,
        output: {
          comments: false,
        },
        ie8: false,
        safari10: false,
      },
    }),
  ];
}

module.exports = webpackConfig;
