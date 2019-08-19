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

import DOMPurify from 'dompurify';

const dom_sanitizer = DOMPurify.sanitize;
const isValidUsingDOMPurify = (val, config) => {
    const clean = dom_sanitizer(val, config);
    return clean === val ? true : false;
};

const NAME = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    },
    getErrorMsg: function() {
      return 'Invalid Input, see input instructions.';
    }
};

const FILE_PATH = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};

const AWS_ACCESS_KEY_ID = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};
const AWS_SECRET_ACCESS_KEY = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};

const GCS_PROJECT_ID = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};

const GCS_BUCKET_ID = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};

/*
- hostname: https://tools.ietf.org/html/rfc1123
    - misses excluding some cases from https://tools.ietf.org/html/rfc2181
*/
const HOSTNAME_1123 = {
    regex: RegExp("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Check Standard for Hostname in RFC-1123",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

const DEFAULT = {
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "cannot contain any xml tags."
    ],
    validate: function(val) {
        return isValidUsingDOMPurify(val, this.allowed);
    },
    getInfo: function() {
        return this.info[0];
    }
};

const types = {
    "DEFAULT": DEFAULT,
    "NAME": NAME,
    "FILE_PATH": FILE_PATH,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "GCS_PROJECT_ID": GCS_PROJECT_ID,
    "GCS_BUCKET_ID": GCS_BUCKET_ID,
    "HOSTNAME_1123": HOSTNAME_1123
};

export default types;
