#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2016 Cask Data, Inc.
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

# Ruby port of Cloudera's Parcel Repository Manifest Creator, available
# here: https://github.com/cloudera/cm_ext

# This program creates a manifest.json file from a directory of parcels and
# places the file in the same directory as the parcels.
# Once created, the directory can be served over http as a parcel repository.

require 'stringio'
require 'digest/sha1'
require 'rubygems/package'
require 'zlib'
require 'json'

def _get_parcel_dirname(parcel_name)
  m = /^(.*?)-(.*)-(.*?)$/.match(parcel_name)
  m[1] + '-' + m[2]
end

def _safe_copy(key, src, dest)
  dest[key] = src[key] if src.key?(key)
end

# port of Python tar.getmember
# Takes a Gem::Package::TarReader and a String
# Returns the matching Gem::Package::TarReader::Entry
def _get_tar_member(tar, entry)
  tar.rewind
  tar.each do |record|
    return record if record.full_name == entry
  end
  fail KeyError, "key #{entry} not found"
end

def make_manifest(path, timestamp = Time.now.to_i)
  manifest = {}
  manifest['lastUpdated'] = timestamp
  manifest['parcels'] = []

  Dir.foreach(path) do |f|
    next unless f.end_with?('.parcel')

    puts "Found parcel #{f}"
    entry = {}
    entry['parcelName'] = f

    fullpath = File.join(path, f)

    entry['hash'] = Digest::SHA1.file(fullpath).hexdigest

    # Open the .tgz parcel file
    Gem::Package::TarReader.new(Zlib::GzipReader.open(fullpath)) do |tar|
      # Get the parcel.json entry
      begin
        json_member = _get_tar_member(tar, File.join(_get_parcel_dirname(f), 'meta', 'parcel.json'))
      rescue KeyError
        puts "Parcel #{f} does not contain parcel.json"
        break
      end

      # Try to parse this entry as JSON
      begin
        parcel = JSON.parse(json_member.read)
      rescue => e
        puts "Failed to parse json: #{e.inspect}"
      end

      _safe_copy('depends', parcel, entry)
      _safe_copy('replaces', parcel, entry)
      _safe_copy('conflicts', parcel, entry)
      _safe_copy('components', parcel, entry)

      # Try to include releaseNotes
      begin
        notes_member = _get_tar_member(tar, File.join(_get_parcel_dirname(f), 'meta', 'release-notes.txt'))
        entry['releaseNotes'] = notes_member.read
      rescue KeyError
        # No problem if there's no release notes
      end
    end
    manifest['parcels'].push(entry)
  end
  JSON.pretty_generate(manifest, indent: '    ')
end

path = Dir.getwd
path = ARGV[0] if ARGV.length > 0
puts "Scanning directory: #{path}"

manifest = make_manifest(path)
File.write(File.join(path, 'manifest.json'), manifest)
