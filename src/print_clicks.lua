#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local cli  = require('cliargs')
local yaml = require('yaml')

local _VERSION = '0.0.1'

cli:set_name('print_events.lua')
cli:add_argument('TAG', 'visitor cookie id')
cli:add_option('-s, --storage=TYPE', 'storage engine type: [redis|mongodb]',
               'redis')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

if args['v'] then
  return print('print_events.lua: version ' .. _VERSION)
end

local storage
local storage_engine = args['s']

if (storage_engine == 'redis') or (storage_engine == 'mongodb') then
  storage = require('storage.' .. storage_engine)
else
  return print('wrong storage type, please see --help')
end

local client = storage.new()
print(yaml.dump(client:get_events(args['TAG'])))
