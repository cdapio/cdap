#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This program merges two Parcel Repository Manifests into a single manifest.json
# See https://github.com/cloudera/cm_ext/wiki/The-parcel-repository-format for
# more information on Parcel Repository Manifests

# This script simply merges the manifests as follows:
#   lastUpdated: the most recent timestamp is used
#   parcels: the arrays are concatenated. An error will be thrown if duplicate parcel
#     names are found in both manifests. It will not try to deep merge.

require 'json'
require 'optparse'

# Parse command line options.
options = {}
begin
  op = OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options] [file1] [file2]"
    opts.on('-m', '--manifest FILE', 'manifest file to merge. Can be specified twice, or once as a comma-separated list') do |m|
      options[:manifest] = [] unless options[:manifest]
      options[:manifest] += m.split(',')
    end
    opts.on('-o', '--output FILE', 'Output manifest file to create. Will overwrite any existing file') do |o|
      options[:output] = o
    end
    opts.on('--overwrite', 'Resolve duplicate parcel entries by preferring first manifest specified') do
      options[:overwrite] = true
    end

    opts.separator ''
    opts.separator 'Required Arguments: must specify two input manifest files, via two -m arguments or two positional parameters'
    opts.separator ''
    opts.separator 'Examples:'
    opts.separator '  # Merges two manifests and writes output to a file:'
    opts.separator "  #{$PROGRAM_NAME} -m /path/to/manifest1 -m /path/to/manifest2 -o /path/to/output"
    opts.separator '  # Equivalent to above:'
    opts.separator "  #{$PROGRAM_NAME} -m /path/to/manifest1,/path/to/manifest2 -o /path/to/output"
    opts.separator '  # Writes to stdout only:'
    opts.separator "  #{$PROGRAM_NAME} /path/to/manifest1 /path/to/manifest2"
    opts.separator '  # Duplicate entries from manifest1 will take precedence over manifest2:'
    opts.separator "  #{$PROGRAM_NAME} /path/to/manifest1 /path/to/manifest2 --overwrite"
    opts.separator ''
  end
  op.parse!(ARGV)
rescue OptionParser::InvalidArgument, OptionParser::InvalidOption
  puts "Invalid Argument/Options: #{$ERROR_INFO}"
  puts op # prints usage
  exit 1
end

# Validate args
# If no -m supplied, but 2 positional parameters, use them
options[:manifest] = ARGV if !options.key?(:manifest) && ARGV.length == 2

# Ensure we have 2 inputs
raise 'Must supply two input manifest.json files using the -m option' unless options.key?(:manifest) && options[:manifest].length == 2

# Convert json file to hash obj
def deserialize_manifest(path)
  JSON.parse(File.read(path))
end

# Convert hash obj to json string
def serialize_manifest(obj)
  JSON.pretty_generate(obj, indent: '    ')
end

# returns common parcelName entries between input manifests
def duplicate_parcelNames(m1, m2)
  m1_parcels = m1['parcels'].map { |h| h['parcelName'] }
  m2_parcels = m2['parcels'].map { |h| h['parcelName'] }

  duplicates = m1_parcels & m2_parcels
end

# Check for duplicate input and fail
def check_for_duplicates(m1, m2)
  duplicates = duplicate_parcelNames(m1, m2)
  raise "Cannot merge, duplicate parcels across manifests: #{duplicates}. Consider using --overwrite option" unless duplicates.empty?
end

# Given two manifest hashes, merge into one
def merge_manifests(m1, m2, overwrite = false)
  if overwrite
    duplicates = duplicate_parcelNames(m1,m2)
    # remove duplicate parcel entries from the 2nd input prior to merge
    m2['parcels'].reject! { |h| duplicates.include? h['parcelName'] }
  else
    # Fail on invalid input
    check_for_duplicates(m1, m2)
  end

  output = {}
  # Set lastUpdated to most recent of the inputs
  output['lastUpdated'] = m1['lastUpdated'] > m2['lastUpdated'] ? m1['lastUpdated'] : m2['lastUpdated']
  # Set parcels to union of inputs
  output['parcels'] = m1['parcels'] | m2['parcels']

  output
end

# Begin main logic
m1 = deserialize_manifest(options[:manifest][0])
m2 = deserialize_manifest(options[:manifest][1])

merged = merge_manifests(m1, m2, options[:overwrite])

if options.key?(:output)
  # Write to specified output file
  File.write(options[:output], serialize_manifest(merged))
else
  # Write to STDOUT only
  puts serialize_manifest(merged)
end
