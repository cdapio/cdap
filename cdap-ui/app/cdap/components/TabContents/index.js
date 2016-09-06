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

import React, {PropTypes, Component} from 'react';

export default class TabContents extends Component {
  constructor(props) {
    super(props);
    this.childRefs = {};
    this.state = {
      activeTab: this.props.activeTab
    };
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      activeTab: nextProps.activeTab
    });
  }
  componentDidMount() {
    var childRefs = Object.keys(this.childRefs);
    if (!childRefs.length) {
      return;
    }
    childRefs.forEach(childRef => {
      this.childRefs[childRef].classList.add('hide');
    });
    if (typeof this.state.activeTab === 'undefined') {
      childRefs[0].classList.remove('hide');
    } else {
      this.childRefs[this.state.activeTab].classList.remove('hide');
    }
  }
  componentDidUpdate() {
    let childRefs = Object.keys(this.childRefs);
    if (!childRefs.length) {
      return;
    }
    // FIXME: Here we are not doing type checking for active tab.
    // Today we don't enforce what 'type' activeTab should be. Ideally a tab could
    // be named using numbers or strings (name of the tab).
    // User uses number for tab names. Object.keys(this.childRefs) will give an array of
    // strings (numbers in strings). Thank you javascript.
    
    let clickedChild = childRefs.filter(childRef => childRef == this.state.activeTab);
    childRefs.forEach(ref => this.childRefs[ref].classList.add('hide'));
    this.childRefs[clickedChild[0]].classList.remove('hide');
  }
  render() {
    return (
      <div className="cask-tab-contents">
        {
          React.Children.map(this.props.children, (child) => {
            return <div
              ref={(ref) => this.childRefs[child.props.name] = ref}
            >
              {child}
            </div>;
          }, this)
        }
      </div>
    );
  }
}
TabContents.propTypes = {
  children: PropTypes.node,
  activeTab: PropTypes.any
};
