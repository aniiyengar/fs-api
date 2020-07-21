
const path = require('path');

const ENV = process.env.NODE_ENV || 'development';

module.exports = {
  mode: ENV,

  entry: './src/index.js',

  target: 'node',

  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'bundle.js',
  },

  module: {
    rules: [
      {
        test: /\.js$/,
        include: [
          path.resolve(__dirname, 'app'),
        ],
        loader: 'babel-loader',
        options: {
          presets: ['env'],
        },
      },
    ],
  },

  externals: require('webpack-node-externals')(),
};
