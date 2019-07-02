/*
 * Copyright © 2019 Cask Data, Inc.
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

const fetch = require('node-fetch').default;
var { ApolloClient } = require("apollo-client");
var { InMemoryCache } = require('apollo-cache-inmemory');
var { HttpLink } = require('apollo-link-http');
var { gql } = require("apollo-boost");

const cache = new InMemoryCache();
const link = new HttpLink({
    uri: 'http://localhost:11011/graphql',
    fetch: fetch
})

const client = new ApolloClient({
    cache,
    link
})

function calling() {
    var start = new Date().getTime();
    while (new Date().getTime() < start + 5000);

    client
        .query({
            query: gql`
    {
      status
    }
  `
        })
        .then(result => log.info(result));
}

module.exports = {
    client,
    calling
}

