# frozen_string_literal: true

require 'rubygems'
require 'bundler/setup'
require 'kinesis'

Bundler.require(:development)
$TESTING = true # rubocop:disable Style/GlobalVars
