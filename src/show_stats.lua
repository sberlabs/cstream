#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-- Show miscellaneous statistics from redis store

local cli     = require('cliargs')
local serpent = require('serpent')
local storage = require('storage')
local zmq     = require('lzmq')

local _VERSION = '0.0.1'

cli:set_name('show_stats.lua')
cli:add_option('-n, --size=N', 'number of users in sample', '1000')
cli:add_flag('-c, --clicksavg', 'print average number of clicks per user' ..
               'use with -n option')
cli:add_flag('-a, --all', 'count all users and clicks (could be slow!)')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

local client = storage.new()
local count = 0
local total = 0

local scanner = function(max_count)
  max_count = max_count or -1
  return function(key)
    local clicks = client:get_clicks(key)
    count = count + 1
    total = total + #clicks
    if (max_count ~= -1) and (count >= max_count) then
      return false
    else
      return true
    end
  end
end

if args['c'] then
  client:scan_clicks(scanner(tonumber(args['n'])))
  print('Sample size: ' .. count .. ' visitors')
  print('Average clicks per visitor: ' .. total/count)
elseif args['a'] then
  client:scan_clicks(scanner())
  print('Total visitors: ' .. count)
  print('Total clicks  : ' .. total)
  print('Average clicks per visitor: ' .. total/count)
elseif args['v'] then
  return print('show_stats.lua: version ' .. _VERSION)
end

