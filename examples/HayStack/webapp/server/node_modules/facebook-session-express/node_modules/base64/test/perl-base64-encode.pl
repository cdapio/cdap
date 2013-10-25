#!/usr/bin/perl
use warnings;
use strict;

use MIME::Base64 qw/encode_base64/;
use Digest::MD5 qw/md5_hex/;
use JSON qw/encode_json/;
use Slurp qw/slurp/;

print encode_json({ map {
    $_ => md5_hex(join '', split /\s+/, encode_base64(scalar slurp($_)))
} glob 'data/*' });
