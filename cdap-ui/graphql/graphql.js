const log4js = require('log4js');
const { ApolloServer } = require('apollo-server-express');
const { importSchema } = require('graphql-import');
const merge = require('lodash/merge')

const log = log4js.getLogger('graphql');
const env = process.env.NODE_ENV;

const { applicationResolvers } = require('./resolvers/applicationResolvers')
const { namespaceResolvers } = require('./resolvers/namespaceResolvers')
const { metadataResolvers } = require('./resolvers/metadataResolvers')
const { programRecordResolvers } = require('./resolvers/programRecordResolvers')
const { programRecordTypeResolvers } = require('./resolvers/type/programRecordTypeResolver')
const { scheduleResolvers } = require('./resolvers/scheduleResolvers')
const { statusResolvers } = require('./resolvers/statusResolvers')

const resolvers = merge(applicationResolvers,
    namespaceResolvers,
    metadataResolvers,
    programRecordTypeResolvers,
    programRecordResolvers,
    scheduleResolvers,
    statusResolvers);

const typeDefs = importSchema('graphql/schema/rootSchema.graphql');

if (typeof resolvers !== 'undefined') {
    log.error("The resolvers are undefined");
}

if (typeof typeDefs === 'undefined') {
    log.error("The type definitions is undefined");
}

const server = new ApolloServer({
    typeDefs, resolvers,
    introspection: env === 'production' ? false : true,
    playground: env === 'production' ? false : true,
});

function applyMiddleware(app) {
    server.applyMiddleware({ app });
}

module.exports = {
    applyMiddleware,
    resolvers,
    typeDefs
};
