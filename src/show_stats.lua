#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-- Show misc stats from events store

package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local cli  = require('cliargs')
local yaml = require('yaml')

local _VERSION = '0.0.1'

cli:set_name('show_stats.lua')
cli:add_option('-s, --storage=TYPE', 'storage engine type: [redis|mongodb]',
               'redis')
cli:add_option('-n, --size=N', 'number of visitors in sample', '1000')
cli:add_flag('-c, --clicksavg', 'print average number of events per visitor,' ..
               ' use with -n option')
cli:add_flag('-a, --all', 'count all visitors and events (could be slow!)')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

local storage
local storage_engine = args['s']

if (storage_engine == 'redis') or (storage_engine == 'mongodb') then
  storage = require('storage.' .. storage_engine)
else
  return print('wrong storage type, please see --help')
end

local client = storage.new()
local count_visitors = 0
local total_events = 0

local function events_array_size(events)
  if storage_engine == 'mongodb' then
    return #events + 1
  end
  return #events
end

local scanner = function(max_count)
  max_count = max_count or -1
  return function(visitor, events)
    events = events or client:get_events(visitor)
    count_visitors = count_visitors + 1
    total_events = total_events + events_array_size(events)
    if (max_count ~= -1) and (count_visitors >= max_count) then
      return false
    else
      return true
    end
  end
end

if args['c'] then
  client:scan_events(scanner(tonumber(args['n'])))
  print('Sample size: ' .. count_visitors .. ' visitors')
  print('Average events per visitor: ' .. total_events/count_visitors)
elseif args['a'] then
  client:scan_events(scanner())
  print('Total visitors: ' .. count_visitors)
  print('Total events  : ' .. total_events)
  print('Average events per visitor: ' .. total_events/count_visitors)
elseif args['v'] then
  return print('show_stats.lua: version ' .. _VERSION)
end
