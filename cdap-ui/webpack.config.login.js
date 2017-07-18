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
var path = require('path');
var plugins = [
  new webpack.DllReferencePlugin({
    context: path.resolve(__dirname, 'dll'),
    manifest: require(path.join(__dirname, 'dll', '/shared-vendor-manifest.json'))
  }),
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
    syntax: 'scss',
    files: ['**/*.scss']
  })
];
var mode = process.env.NODE_ENV;
var loaders = [
  {
    test: /\.scss$/,
    loader: 'style-loader!css-loader!sass-loader'
  },
  {
    test: /\.ya?ml$/,
    loader: 'yml'
  },
  {
    test: /\.css$/,
    loader: 'style-loader!css-loader!sass-loader'
  },
  {
    test: /\.js$/,
    loader: 'babel',
    exclude: /node_modules/
  },
  {
    test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    loader: 'url-loader?limit=10000&mimetype=application/font-woff'
  },
  {
    test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    loader: 'file-loader'
  }
];
var webpackConfig = {
  context: __dirname + '/app/login',
  entry: {
    'login': ['babel-polyfill', './login.js']
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
  stats: {
    chunks: false
  },
  output: {
    filename: './[name].js',
    path: __dirname + '/login_dist/login_assets'
  },
  plugins: plugins
};

if (mode === 'production') {
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
