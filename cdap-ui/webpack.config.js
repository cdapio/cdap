/*
 * Copyright © 2016 Cask Data, Inc.
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
var CopyWebpackPlugin = require('copy-webpack-plugin');
var path = require('path');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
var commonPlugins =
[
  new webpack.optimize.CommonsChunkPlugin("common", "common.js", Infinity),
  new LodashModuleReplacementPlugin,
  new webpack.optimize.DedupePlugin()
];
var mode = process.env.NODE_ENV;
if (mode === 'production' || mode === 'build') {
  commonPlugins.push(
    new webpack.DefinePlugin({
      'process.env':{
        'NODE_ENV': JSON.stringify("production"),
        '__DEVTOOLS__': false
      },
    }),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
          warnings: false
      }
    })
  );
}
var cdapPlugins = commonPlugins.concat(new CopyWebpackPlugin([
  {
    from: './cdap.html',
    to: './cdap.html'
  },
  {
    from: './styles/fonts',
    to: './fonts/'
  },
  {
    from: path.resolve(__dirname, 'node_modules', 'font-awesome', 'fonts'),
    to: './fonts/'
  },
  {
    from: './styles/img',
    to: './img/'
  }
]));
var loginPlugins = commonPlugins.concat(new CopyWebpackPlugin([
  {
    from: './login.html',
    to: './login.html'
  },
  {
    from: './styles/fonts',
    to: './fonts/'
  },
  {
    from: './styles/img',
    to: './img/'
  }
]));

var commonLoaders = [
  {
    test: /\.less$/,
    loader: 'style-loader!css-loader!less-loader'
  },
  {
    test: /\.js$/,
    loader: 'babel',
    exclude: /node_modules/,
    query: {
      plugins: ['lodash'],
      presets: ['react', 'es2015']
    }
  }
];

const cdapWebpackConfig = {
  context: __dirname + '/app/cdap',
  entry: {
    'cdap': ['./cdap.js'],
    'common': [
      'whatwg-fetch',
      'react',
      'react-dom',
      'redux',
      'lodash',
      'classnames',
      'node-uuid',
      'sockjs-client',
      'rx'
    ]
  },
  module: {
    preLoaders: [
      {
        test: /\.js$/,
        loader: 'eslint-loader',
        exclude: [
          /node_modules/,
          /bower_components/,
          /dist/,
          /cdap_dist/
        ]
      }
    ],
    loaders: commonLoaders
  },
  output: {
    filename: './[name].js',
    path: __dirname + '/cdap_dist/cdap_assets'
  },
  plugins: cdapPlugins
};
const loginWebpackConfig = {
  context: __dirname + '/app/login',
  entry: {
    'login': ['./login.js'],
    'common': ['react', 'react-dom']
  },
  module: {
    preLoaders: [
      {
        test: /\.js$/,
        loader: 'eslint-loader',
        exclude: [
          /node_modules/,
          /bower_components/,
          /dist/,
          /cdap_dist/
        ]
      }
    ],
    loaders: commonLoaders
  },
  output: {
    filename: './[name].js',
    path: __dirname + '/login_dist/login_assets'
  },
  plugins: loginPlugins
};
module.exports = [
  cdapWebpackConfig,
  loginWebpackConfig
];
