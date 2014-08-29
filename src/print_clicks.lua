#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

local cli     = require('cliargs')
local serpent = require('serpent')
local storage = require('storage')

local _VERSION = '0.0.1'

cli:set_name('print_clicks.lua')
cli:add_argument('TAG', 'user tag id')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

if args['v'] then
  return print('getclicks.lua: version ' .. _VERSION)
end

local client = storage.new()
print(serpent.block(client:get_clicks(args['TAG'])))
