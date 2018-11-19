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
var path = require('path');
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
var UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const CleanWebpackPlugin = require('clean-webpack-plugin');
var ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
let pathsToClean = ['common_dist'];

// the clean options to use
let cleanOptions = {
  verbose: true,
  dry: false,
};
var mode = process.env.NODE_ENV || 'production';
const isModeProduction = (mode) => mode === 'production' || mode === 'non-optimized-production';

var plugins = [
  new LodashModuleReplacementPlugin({
    shorthands: true,
    collections: true,
    caching: true,
  }),
  new CleanWebpackPlugin(pathsToClean, cleanOptions),
  new CaseSensitivePathsPlugin(),
  // by default minify it.
  new webpack.DefinePlugin({
    'process.env': {
      NODE_ENV: isModeProduction(mode)
        ? JSON.stringify('production')
        : JSON.stringify('development'),
    },
  }),
];

if (!isModeProduction(mode)) {
  plugins.push(
    new ForkTsCheckerWebpackPlugin({
      tsconfig: __dirname + '/tsconfig.json',
      tslint: __dirname + '/tslint.json',
      tslintAutoFix: true,
      // watch: ["./app/cdap"], // optional but improves performance (less stat calls)
      memoryLimit: 4096,
    })
  );
}

var rules = [
  {
    test: /\.scss$/,
    use: ['style-loader', 'css-loader', 'sass-loader'],
  },
  {
    test: /\.ya?ml$/,
    use: 'yml-loader',
  },
  {
    test: /\.css$/,
    use: ['style-loader', 'css-loader', 'sass-loader'],
  },
  {
    enforce: 'pre',
    test: /\.js$/,
    loader: 'eslint-loader',
    options: {
      fix: true,
    },
    exclude: [
      /node_modules/,
      /bower_components/,
      /dist/,
      /old_dist/,
      /cdap_dist/,
      /common_dist/,
      /lib/,
      /wrangler_dist/,
    ],
    include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
  },
  {
    test: /\.js$/,
    use: 'babel-loader',
    exclude: /node_modules/,
  },
  {
    test: /\.tsx?$/,
    use: [
      'babel-loader',
      {
        loader: 'ts-loader',
        options: {
          transpileOnly: true,
        },
      },
    ],
    exclude: [/node_modules/, /lib/],
  },
  {
    test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: [
      {
        loader: 'url-loader',
        options: {
          limit: 10000,
          mimetype: 'application/font-woff',
        },
      },
    ],
  },
  {
    test: /\.(ttf|eot)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: 'url-loader',
  },
  {
    test: /\.svg/,
    use: [
      {
        loader: 'svg-sprite-loader',
      },
    ],
  },
];
if (isModeProduction(mode)) {
  plugins.push(
    new UglifyJsPlugin({
      uglifyOptions: {
        ie8: false,
        compress: {
          warnings: false,
        },
        output: {
          comments: false,
          beautify: false,
        },
      },
    })
  );
}
var webpackConfig = {
  mode: isModeProduction(mode) ? 'production' : 'development',
  context: __dirname + '/app/common',
  optimization: {
    splitChunks: {
      minChunks: Infinity,
    },
  },
  entry: {
    'common-new': ['./cask-shared-components.js'],
    'common-lib-new': [
      '@babel/polyfill',
      'classnames',
      'reactstrap',
      'i18n-react',
      'sockjs-client',
      'react-dropzone',
      'react-redux',
      'svg4everybody',
      'numeral',
    ],
  },
  module: {
    rules,
  },
  stats: {
    assets: false,
    children: false,
    chunkGroups: false,
    chunkModules: false,
    chunkOrigins: false,
    chunks: false,
    modules: false,
  },
  output: {
    filename: '[name].js',
    chunkFilename: '[name].[chunkhash].js',
    path: __dirname + '/common_dist',
    library: 'CaskCommon',
    libraryTarget: 'umd',
    publicPath: '/common_assets/',
  },
  externals: {
    react: {
      root: 'React',
      commonjs2: 'react',
      commonjs: 'react',
      amd: 'react',
    },
    'react-dom': {
      root: 'ReactDOM',
      commonjs2: 'react-dom',
      commonjs: 'react-dom',
      amd: 'react-dom',
    },
    'react-addons-css-transition-group': {
      commonjs: 'react-addons-css-transition-group',
      commonjs2: 'react-addons-css-transition-group',
      amd: 'react-addons-css-transition-group',
      root: ['React', 'addons', 'CSSTransitionGroup'],
    },
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx'],
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      wrangler: __dirname + '/app/wrangler',
      styles: __dirname + '/app/cdap/styles',
    },
  },
  plugins,
};

module.exports = webpackConfig;
