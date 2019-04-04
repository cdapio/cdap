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

import * as React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MySecureKeyApi } from 'api/securekey';
import TextField from '@material-ui/core/TextField';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import DeleteIcon from '@material-ui/icons/Delete';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import Paper from '@material-ui/core/Paper';

interface IState {
  secureKeys: any[];
  name: string;
  description: string;
  data: string;
}

// Currently a hidden link to internally manage secure keys
export default class SecureKeys extends React.PureComponent<{}, IState> {
  public state = {
    secureKeys: [],
    name: '',
    description: '',
    data: '',
  };

  public componentDidMount() {
    this.fetchSecureKeys();
  }

  private fetchSecureKeys = () => {
    const namespace = getCurrentNamespace();

    MySecureKeyApi.list({ namespace }).subscribe((res) => {
      this.setState({
        secureKeys: res,
      });
    });
  };

  private handleChange = (property) => {
    return (e) => {
      // @ts-ignore
      this.setState({
        [property]: e.target.value,
      });
    };
  };

  private addKey = () => {
    const { name, description, data } = this.state;

    if (!name || !data) {
      return;
    }

    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      key: name,
    };

    const requestBody = {
      description,
      data,
    };

    MySecureKeyApi.add(params, requestBody).subscribe(() => {
      this.setState({
        name: '',
        description: '',
        data: '',
      });
      this.fetchSecureKeys();
    });
  };

  private deleteKey = (key) => {
    const namespace = getCurrentNamespace();
    const params = {
      namespace,
      key,
    };

    MySecureKeyApi.delete(params).subscribe(this.fetchSecureKeys);
  };

  public render() {
    return (
      <div className="secure-keys-container container">
        <h1>Secure Keys</h1>

        <div className="key-input">
          <TextField
            label="Name"
            value={this.state.name}
            onChange={this.handleChange('name')}
            margin="normal"
            fullWidth={true}
            variant="outlined"
          />

          <TextField
            label="Description"
            value={this.state.description}
            onChange={this.handleChange('description')}
            margin="normal"
            fullWidth={true}
            variant="outlined"
          />

          <TextField
            label="data"
            value={this.state.data}
            onChange={this.handleChange('data')}
            margin="normal"
            fullWidth={true}
            multiline={true}
            variant="outlined"
          />

          <Button variant="contained" color="primary" onClick={this.addKey}>
            Add Secure Key
          </Button>
        </div>

        <hr />

        <div className="key-lists">
          <Paper>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Key</TableCell>
                  <TableCell>Description</TableCell>
                  <TableCell />
                </TableRow>
              </TableHead>
              <TableBody>
                {this.state.secureKeys.map((key) => (
                  <TableRow key={key.name}>
                    <TableCell>{key.name}</TableCell>
                    <TableCell>{key.description}</TableCell>
                    <TableCell>
                      <IconButton color="secondary" onClick={this.deleteKey.bind(this, key.name)}>
                        <DeleteIcon />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Paper>
        </div>
      </div>
    );
  }
}
