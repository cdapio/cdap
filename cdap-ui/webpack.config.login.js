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
var StyleLintPlugin = require('stylelint-webpack-plugin');
var plugins = [
  new webpack.optimize.CommonsChunkPlugin("common", "common.js", Infinity),
  new webpack.optimize.DedupePlugin(),
  new CopyWebpackPlugin([
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
  ]),
  new StyleLintPlugin({
    syntax: 'less',
    files: ['**/*.less']
  })
];
var mode = process.env.NODE_ENV;
if (mode === 'production' || mode === 'build') {
  plugins.push(
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
var loaders = [
  {
    test: /\.less$/,
    loader: 'style-loader!css-loader!less-loader'
  },
  {
    test: /\.ya?ml$/,
    loader: 'yml'
  },
  {
    test: /\.css$/,
    loader: 'style-loader!css-loader!less-loader'
  },
  {
    test: /\.js$/,
    loader: 'babel',
    exclude: /node_modules/,
    query: {
      presets: ['react', 'es2015']
    }
  }
];

module.exports = {
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
          /cdap_dist/,
          /common_dist/,
          /wrangler_dist/
        ]
      }
    ],
    loaders: loaders
  },
  output: {
    filename: './[name].js',
    path: __dirname + '/login_dist/login_assets'
  },
  plugins: plugins
};
