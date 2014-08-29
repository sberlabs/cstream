#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-- Synchronized click stream subscriber

require('ml').import()
require('zhelpers')
local cjson   = require('cjson.safe')
local cli     = require('cliargs')
local serpent = require('serpent')
local statsd  = require('statsd')({ host='127.0.0.1',
                                   port=8125,
                                   namespace='cstream.stats' })
local storage = require('storage')
local zmq     = require('lzmq')

local _VERSION = '0.0.1'

cli:set_name('cstream_sub.lua')
cli:add_argument('ADDR', 'click stream publisher address')
cli:add_argument('PORT', 'click stream publisher base port')
cli:add_option('-p, --provider=NAME',
               "provider's module name to import data into redis", 'rutarget')
cli:add_flag('-s, --sync', 'send sync request to publisher before subscribing')
cli:add_flag('-x, --dryrun', 'do not store clicks, just receive them')
cli:add_flag('-d, --debug', 'receiver will run in debug mode')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()

if not args then
  return
end

if args['v'] then
  return print('cstream_sub.lua: version ' .. _VERSION)
end

local addr = args['ADDR']
local port = tonumber(args['PORT'])

local context = zmq.context()

local subscriber, err = context:socket{zmq.SUB,
                                       subscribe = '',
                                       connect = 'tcp://' .. addr .. ':' ..
                                         tostring(port)
}
zassert(subscriber, err)

if args['s'] then
  -- sned sync request to publisher and wait for reply
  sleep(1)
  local syncclient = context:socket{zmq.REQ,
                                    connect = 'tcp://' .. addr .. ':' ..
                                      tostring(port+1)}
  zassert(syncclient, err)
  syncclient:send('') -- send a synchronization request
  syncclient:recv() -- wait for synchronization reply
  if args['d'] then printf("Synchronized\n") end
end

local dry_run = args['x']
local provider = require(args['p'])
local client = storage.new()

if not client then
  return printf("error connecting to database: %s\n", err)
end

local msg_nbr = 0
local valid_nbr = 0

if args['d'] then printf("Waiting for incoming messages\n") end

while true do
  local msg = subscriber:recv()
  if msg == "END" then break end
  local click, err = provider.parse_message(msg)
  local id = click.id
  if click then
    if not dry_run then
      client:save_click(click)
    end
    if args['d'] then
      print(serpent.block(client:get_clicks(id)))
    end
    statsd:increment('clicks')
    valid_nbr = valid_nbr + 1
  end
  msg_nbr = msg_nbr + 1
end

if args['d'] then
  printf("Received %d messages, valid %d messages\n", msg_nbr, valid_nbr)
end
