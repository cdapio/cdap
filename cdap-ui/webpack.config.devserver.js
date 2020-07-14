const path = require("path")
const webpack = require('webpack')
const { merge } = require('webpack-merge');
const common = require('./webpack.config.js')

module.exports = merge(common, {
  mode: 'development',
  devtool: "inline-source-map",
  watch: true,
  devServer: {
    contentBase: path.join(__dirname, "dist"),
    port: 8080,
    stats: "minimal",
    watchContentBase: true,
    historyApiFallback: true,
    open: false,
    hot: false
  }
});