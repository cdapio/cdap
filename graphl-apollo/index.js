const { ApolloServer } = require('apollo-server');
const { importSchema } = require('graphql-import');
const { resolvers } = require('./resolver')

const typeDefs  = importSchema('schema/test.graphql')

if(typeof typeDefs === 'undefined') {
  throw "The type definitions is undefined"
}

if(typeof resolvers === 'undefined') {
  throw "The resolvers are undefined"
}

const server = new ApolloServer({ typeDefs, resolvers });

server.listen().then(({ url }) => {
  console.log(`🚀  Server ready at ${url}`);
});