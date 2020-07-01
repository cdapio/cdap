/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';
const SiblingCommunicationContext = React.createContext(null);
const SiblingCommunicationConsumer = SiblingCommunicationContext.Consumer;

/**
 * Each row has a sibling line for hierarchical relationship with parent.
 * There could be multiple lines depending on the number of ancestors a particular
 * row is nested under.
 *
 * The context provides a way to broadcast state changes on the siblingline. Stage changes
 * happen if user hovers over the line to see the hierarchy with immediate parent.
 *
 * In this case the provider broadcasts the current hovered parent-sibling line and that gets highlighted.
 */
class SiblingCommunicationProvider extends React.Component {
  public setActiveParent = (id) => {
    this.setState({
      activeParent: id,
    });
  };
  public state = {
    activeParent: null,
    setActiveParent: this.setActiveParent.bind(this),
  };

  public render() {
    return (
      <SiblingCommunicationContext.Provider value={this.state}>
        {this.props.children}
      </SiblingCommunicationContext.Provider>
    );
  }
}

export { SiblingCommunicationProvider, SiblingCommunicationConsumer };
