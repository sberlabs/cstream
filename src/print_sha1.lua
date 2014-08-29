#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

local cli     = require('cliargs')
local crypto  = require('crypto')
local digest  = crypto.digest

local _VERSION = '0.0.1'

cli:set_name('sha1.lua')
cli:add_argument('STRING', 'sha1 of this string will be calculated and printed')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

if args['v'] then
  return print('getclicks.lua: version ' .. _VERSION)
end

local d = digest.new('sha1')
print(d:final(args['STRING']))

