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
var mode = process.env.NODE_ENV;
var plugins = [
  new webpack.DefinePlugin({
    'process.env':{
      'NODE_ENV': JSON.stringify("production"),
      '__DEVTOOLS__': false
    },
  }),
  new webpack.DllPlugin({
    path: path.join(__dirname, 'dll', 'shared-[name]-manifest.json'),
    name: 'shared_[name]',
    context: path.resolve(__dirname, 'dll')
  })
];
var webpackConfig = {
  entry: {
    vendor: [
      'babel-polyfill',
      'react',
      'react-dom',
      'redux',
      'lodash',
      'classnames',
      'reactstrap',
      'i18n-react',
      'react-cookie',
      'whatwg-fetch',
      'react-vis'
    ]
  },
  output: {
    path: path.join(__dirname, 'dll'),
    filename: 'dll.shared.[name].js',
    library: 'shared_[name]'
  },
  stats: {
    chunks: false
  },
  plugins,
  resolve: {
    modules: ['node_modules']
  }
};

if (mode === 'production') {
  plugins.push(
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false
      }
    })
  );
  webpackConfig = Object.assign({}, webpackConfig, {
    plugins
  });
}
if (mode !== 'production') {
  webpackConfig = Object.assign({}, webpackConfig, {
    devtool: 'source-map'
  });
}

module.exports = webpackConfig;
