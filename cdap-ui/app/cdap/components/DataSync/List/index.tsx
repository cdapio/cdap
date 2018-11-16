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
import { db } from 'components/DataSync/firebase';
import Button from '@material-ui/core/Button';
import AddIcon from '@material-ui/icons/Add';
import { Link } from 'react-router-dom';

const COLLECTION = 'instances';

const CreateLink = (props) => <Link to="/datasync/create" {...props} />;

export default class List extends React.PureComponent {
  public state = {
    list: [],
  };

  public componentDidMount() {
    this.fetchInstances();
  }

  private fetchInstances = () => {
    db.collection(COLLECTION)
      .get()
      .then((querySnapshot) => {
        const list = [];
        querySnapshot.forEach((doc) => {
          list.push({
            id: doc.id,
            data: doc.data(),
          });
        });

        this.setState({
          list,
        });
      });
  };

  public render() {
    return (
      <div>
        <h1>Data Sync</h1>

        <div>
          <Button variant="fab" color="primary" component={CreateLink}>
            <AddIcon />
          </Button>
        </div>

        <div className="list">
          {this.state.list.map((item) => {
            return (
              <div className="row" key={item.id}>
                {item.id}
              </div>
            );
          })}
        </div>
      </div>
    );
  }
}
