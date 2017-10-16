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
var LiveReloadPlugin = require('webpack-livereload-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
var HtmlWebpackPlugin = require('html-webpack-plugin');
var StyleLintPlugin = require('stylelint-webpack-plugin');
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
var shortid = require('shortid');
var plugins = [
  new CaseSensitivePathsPlugin(),
  new webpack.DllReferencePlugin({
    context: path.resolve(__dirname, 'dll'),
    manifest: require(path.join(__dirname, 'dll', 'shared-vendor-manifest.json'))
  }),
  new webpack.DllReferencePlugin({
    context: path.resolve(__dirname, 'dll'),
    manifest: require(path.join(__dirname, 'dll', 'cdap-vendor-manifest.json'))
  }),
  new LodashModuleReplacementPlugin({
    shorthands: true,
    collections: true,
    caching: true
  }),
  new CopyWebpackPlugin([
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
  ]),
  new StyleLintPlugin({
    syntax: 'scss',
    files: ['**/*.scss']
  }),
  new HtmlWebpackPlugin({
    title: 'CDAP',
    template: './cdap.html',
    filename: 'cdap.html',
    hash: true,
    hashId: shortid.generate()
  })
];
var mode = process.env.NODE_ENV;

var rules = [
  {
    test: /\.scss$/,
    use: [
      'style-loader',
      'css-loader',
      'postcss-loader',
      'sass-loader'
    ]
  },
  {
    test: /\.ya?ml$/,
    use: 'yml-loader'
  },
  {
    test: /\.json$/,
    use: 'json-loader'
  },
  {
    test: /\.css$/,
    use: [
      'style-loader',
      'css-loader',
      'postcss-loader',
      'sass-loader'
    ]
  },
  {
    enforce: 'pre',
    test: /\.js$/,
    use: 'eslint-loader',
    exclude: [
      /node_modules/,
      /bower_components/,
      /dist/,
      /old_dist/,
      /cdap_dist/,
      /common_dist/,
      /lib/,
      /wrangler_dist/
    ]
  },
  {
    test: /\.js$/,
    use: 'babel-loader',
    exclude: [
      /node_modules/,
      /lib/
    ],
    include: [
      path.join(__dirname, 'app')
    ]
  },
  {
    test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: [
      {
        loader: 'url-loader',
        options: {
          limit: 10000,
          mimetype: 'application/font-woff'
        }
      }
    ]
  },
  {
    test: /\.(ttf|eot)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: 'url-loader'
  },
  {
    test: /\.svg/,
    use: [
      {
        loader: 'svg-sprite-loader',
        options: {
          prefixize: false
        }
      }
    ]
  }
];

var webpackConfig = {
  cache: true,
  context: __dirname + '/app/cdap',
  entry: {
    // including babel-polyfill is temporary as of now. Once babel handles adding https://github.com/babel/babel/issues/4169.
    'cdap': ['babel-polyfill', './cdap.js', 'rx', 'rx-dom']
  },
  module: {
    rules
  },
  output: {
    filename: '[name].js',
    chunkFilename: '[name]-[chunkhash].js',
    path: __dirname + '/cdap_dist/cdap_assets/',
    publicPath: '/cdap_assets/'
  },
  stats: {
    chunks: false,
    chunkModules: false
  },
  plugins: plugins,
  resolve: {
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      lib: __dirname + '/app/lib'
    }
  }
};
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
  webpackConfig = Object.assign({}, webpackConfig, {
    plugins
  });
}

if (mode !== 'production') {
  webpackConfig = Object.assign({}, webpackConfig, {
    devtool: 'source-map',
    plugins:  plugins.concat([
      new LiveReloadPlugin({
        port: 35728,
        appendScriptTag: true
      })
    ])
  });
}


module.exports = webpackConfig;
