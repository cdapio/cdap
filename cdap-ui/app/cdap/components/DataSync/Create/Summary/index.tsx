/*
 * Copyright © 2018 Cask Data, Inc.
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
import { connect } from 'react-redux';

const SummaryView: React.SFC<IProps> = ({ config }) => {
  return (
    <div className="summary">
      <pre>{JSON.stringify(config, null, 2)}</pre>
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    config: state.datasync,
  };
};

const Summary = connect(mapStateToProps)(SummaryView);

export default Summary;
