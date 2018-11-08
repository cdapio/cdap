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
import * as Loadable from 'react-loadable';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
const SubTSXComponent = Loadable({
  loader: () =>
    import(/* webpackChunkName: "ChildTSXComponent" */ 'components/SampleTSXComponent/ChildTSXComponent'),
  loading: LoadingSVGCentered,
});

interface IFCProps {
  prop1: boolean;
  prop2: string;
}

const FunctionalComponent: React.SFC<IFCProps> = ({ prop1, prop2 }) => {
  return (
    <React.Fragment>
      <h4> Stateless component </h4>
      <span> Props: </span>
      <pre>
        {`${prop1}`} : {prop2}
      </pre>
      <hr />
      <SubTSXComponent />
    </React.Fragment>
  );
};

interface IStatefulComponentProps {
  prop3: string;
}

class StatefullComponent extends React.PureComponent<IStatefulComponentProps, {}> {
  public render() {
    return (
      <React.Fragment>
        <h4> Stateful component </h4>
        <span> Props: </span>
        <pre>{this.props.prop3}</pre>
      </React.Fragment>
    );
  }
}
export default function SampleTSXComponent() {
  return [
    <h1 key="super-title"> Hello from TSX! </h1>,
    <FunctionalComponent prop1={true} prop2="Nice!" />,
    <StatefullComponent prop3="hurray!! stateful prop" />,
  ];
}
