#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-- Synchronized click stream publisher

package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

require('ml').import()
require('zhelpers')

local cjson  = require('cjson.safe')
local cli    = require('cliargs')
local inih   = require('inih')
local socket = require('socket')
local yaml   = require('yaml')
local zmq    = require('lzmq')

local _VERSION = '0.0.1'

local function trim(s)
  return string.match(s, '^%s*(.-)%s*$')
end

local function tsv_to_json(fmt, sep, line)
  local data = {}
  local pos = 1

  for value in string.gmatch(line, '[^' .. sep ..']+') do
    value = trim(value)
    if fmt[pos].typ == 'number' then
      data[fmt[pos].name] = tonumber(value)
    elseif fmt[pos].typ == 'string' then
      data[fmt[pos].name] = value
    elseif fmt[pos].typ == 'qstring' then
      data[fmt[pos].name] = string.match(value, '"(.*)"')
    elseif fmt[pos].typ == 'boolean' then
      data[fmt[pos].name] = not not trim(value)
    else
      printf('Unknown data type: %s', fmt[pos].typ)
      os.exit(1)
    end
    pos = pos + 1
  end

  json, err = cjson.encode(data)

  if json then
    return json
  else
    printf('error encoding into json: %s\ntsv line: \n%s\n', err, line)
    os.exit(1)
  end
end

local function parse_ini(filename)
  local data = {}
  assert(inih.parse(filename, function(section, name, value)
    if section == 'tsv' then
      local i, typ = string.match(value, '^(%d+),(%a+)$')
      data[tonumber(i)] = { name=name, typ=typ }
    end
    return true
  end))
  return data
end

cli:set_name('cstream_pub.lua')
cli:add_argument('PORT', 'base port for click stream publishing')
cli:add_option('-i, --inputfile=FILE',
               'read click stream from a FILE')
cli:add_option('-u, --udp=UDP_PORT',
               'receive click stream on udp://localhost:UDP_PORT')
cli:add_option('-f, --format=INI_FILE',
               'tsv format definition file name', 'cstream.ini')
cli:add_option('-s, --subscribers=SUBS',
               'the number of expected subscribers', '1')
cli:add_flag('-d, --debug', 'receiver will run in debug mode')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

if args['v'] then
  return print('cstream_pub.lua: version ' .. _VERSION)
end

local port = tonumber(args['PORT'])

if (args['i'] ~= '') and (args['u'] ~= '') then
  return print('-i and -u options are mutually exclusive, please choose only one')
end

if not((args['i'] ~= '') or (args['u'] ~= '')) then
  return print('please select the source of the click stream using either -i or -u option')
end

if (args['i'] ~= '') and not exists(args['i']) then
  return print('file not found: ' .. args['i'])
end

if (args['f'] ~= '') and not exists(args['f']) then
  return print('file not found: ' .. args['f'])
end

if (args['u'] ~= '') and (tonumber(args['u']) < 1) then
  return print('invalid udp port number')
end

local subscribers_expected = tonumber(args['s'])

if subscribers_expected < 0 then
  return print('invalid number of subscribers')
end

local fmt = parse_ini(args['f'])
if args['d'] then
  printf('%s parsed into the following table:\n%s\n', args['f'], yaml.dump(fmt))
end

local context = zmq.context()
local publisher, err = context:socket{zmq.PUB,
  sndhwm = 1100000,
  bind   = 'tcp://*:' .. tostring(port)
}
zassert(publisher, err)
local syncservice, err = context:socket{zmq.REP,
                                        bind = 'tcp://*:' .. tostring(port + 1)
}
zassert(syncservice, err)

-- Get synchronization from subscribers
if args['d'] then printf("Waiting for subscribers\n") end
local subscribers = 0;

while subscribers < subscribers_expected do
  syncservice:recv()   -- wait for synchronization request
  syncservice:send('') -- send synchronization reply
  subscribers = subscribers + 1
end

if args['d'] then printf("Broadcasting messages\n") end

if args['i'] ~= '' then
  -- read click stream from a local file
  local msg_num = 0

  for line in io.lines(args['i']) do
    msg = tsv_to_json(fmt, '\t', line)
    publisher:send(msg)
    msg_num = msg_num + 1
    if args['d'] then printf('%d: %s\n', msg_num, msg) end
  end

  publisher:send("END")
  if args['d'] then printf('Sent %d messages\n', msg_num) end
end

if args['u'] ~= '' then
  -- receive click stream on udp port
  local msg_num = 0
  local udp = socket.udp()
  udp:setsockname('127.0.0.1', tonumber(args['u']))

  while true do
    local buffer = udp:receive()
    for line in string.gmatch(buffer, '([^\n]*)\n') do
      msg = tsv_to_json(fmt, '\t', line)
      publisher:send(msg)
      msg_num = msg_num + 1
      if args['d'] then printf('%d: %s\n', msg_num, msg) end
    end
  end
end

