/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
require('./NodesMetricsGraph.scss');
/*
   - Better name
   - Name says LogsMetricsGraph but we are passing in runs. Its logs metrics (warning, error, info etc.,) per run. but ugh..
*/
export default class NodesMetricsGraph extends Component {
  constructor(props) {
    super(props);
  }
  render() {
    return (
      <div className="nodes-metrics-graph">
        <pre>{JSON.stringify(this.props.runs, null, 2)}</pre>
      </div>
    );
  }
}
NodesMetricsGraph.propTypes = {
  runs: PropTypes.arrayOf(PropTypes.object)
};
