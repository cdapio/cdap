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
var mode = process.env.NODE_ENV;
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');

var plugins = [
  new CaseSensitivePathsPlugin(),
  new webpack.optimize.CommonsChunkPlugin({
    name: "common-lib",
    fileName: "common-lib.js",
    minChunks: Infinity
  }),
  // by default minify it.
  new webpack.DefinePlugin({
    'process.env':{
      'NODE_ENV': JSON.stringify("production"),
      '__DEVTOOLS__': false
    },
  })
];
var rules = [
  {
    test: /\.scss$/,
    use: [
      'style-loader',
      'css-loader',
      'sass-loader'
    ]
  },
  {
    test: /\.ya?ml$/,
    use: 'yml-loader'
  },
  {
    test: /\.css$/,
    use: [
      'style-loader',
      'css-loader',
      'sass-loader'
    ]
  },
  {
    test: /\.json$/,
    use: 'json-loader'
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
      /login_dist/
    ]
  },
  {
    test: /\.js$/,
    use: 'babel-loader',
    exclude: /node_modules/
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
  context: __dirname + '/app/common',
  entry: {
    'common': ['./cask-shared-components.js'],
    'common-lib': [
      'babel-polyfill',
      'classnames',
      'reactstrap',
      'i18n-react',
      'sockjs-client',
      'rx',
      'rx-dom',
      'react-dropzone',
      'react-redux',
      'svg4everybody',
      'numeral'
    ]
  },
  module: {
    rules
  },
  output: {
    filename: './[name].js',
    chunkFilename: '[name].js',
    path: __dirname + '/common_dist',
    library: 'CaskCommon',
    libraryTarget: 'umd',
    publicPath: '/common_assets/'
  },
  externals: {
    'react': {
      root: 'React',
      commonjs2: 'react',
      commonjs: 'react',
      amd: 'react'
    },
    'react-dom': {
      root: 'ReactDOM',
      commonjs2: 'react-dom',
      commonjs: 'react-dom',
      amd: 'react-dom'
    },
    'react-addons-css-transition-group': {
      commonjs: 'react-addons-css-transition-group',
      commonjs2: 'react-addons-css-transition-group',
      amd: 'react-addons-css-transition-group',
      root: ['React','addons','CSSTransitionGroup']
    },
    'react-addons-transition-group': {
      commonjs: 'react-addons-transition-group',
      commonjs2: 'react-addons-transition-group',
      amd: 'react-addons-transition-group',
      root: ['React','addons','TransitionGroup']
    }
  },
  resolve: {
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      wrangler: __dirname + '/app/wrangler'
    }
  },
  plugins
};
if (mode !== 'production') {
  webpackConfig = Object.assign({}, webpackConfig, {
    devtool: 'source-map'
  });
}
if (mode === 'production') {
  plugins.push(
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false
      },
      output: {
        comments: false
      }
    })
  );
  webpackConfig = Object.assign({}, webpackConfig, {
    plugins
  });
}
module.exports = webpackConfig;
