const { ApolloServer } = require('apollo-server');
const { typeDefs } = require('./schema')
const { resolvers } = require('./resolver')

//var request = require('request'),
//  fs = require('fs'),
//  log4js = require('log4js');


if(typeof typeDefs === 'undefined') {
  throw "The type definitions is undefined"
}

if(typeof resolvers === 'undefined') {
  throw "The resolvers are undefined"
}

//const res = request('http://127.0.0.1:11015/v3/namespaces',
//      function (error, response, body) {
////        console.log('body:', body); // Print the HTML for the Google homepage.
//      });
//
//console.log(res)

const server = new ApolloServer({ typeDefs, resolvers });

server.listen().then(({ url }) => {
  console.log(`🚀  Server ready at ${url}`);
});