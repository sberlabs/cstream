#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

require('zhelpers')

local cjson    = require('cjson.safe')
local cli      = require('cliargs')
local inih     = require('inih')
local serpent  = require('serpent')
local statsd   = require('statsd')({ host='127.0.0.1',
                                     port=8125,
                                     namespace='cstream' })
local yaml     = require('yaml')

local zmq      = require('lzmq')
local zthreads = require('lzmq.threads')
local ztimer   = require('lzmq.timer')
local zpoller  = require('lzmq.poller')

local _VERSION = '0.0.1'

local FRONTEND = "ipc:///tmp/frontend.ipc"
local BACKEND  = "ipc:///tmp/backend.ipc"

local tremove = table.remove
local tinsert = table.insert

local function trim(s)
  return string.match(s, '^%s*(.-)%s*$')
end

local function parse_ini(filename)
  local data = {}
  assert(inih.parse(filename, function(section, name, value)
    local section, subsection = string.match(section, '([%w_]+):*([%w_]*)')
    section = trim(section)
    if subsection ~= '' then subsection = trim(subsection) end
    if not data[section] then data[section] = {} end
    if subsection ~= '' then
      if not data[section][subsection] then data[section][subsection] = {} end
      data[section][subsection][name] = trim(value)
    else
      data[section][name] = trim(value)
    end
    return true
  end))
  return data
end

cli:set_name('cstream.lua')
cli:add_option('-f, --inifile=FILE', 'configuration file name', 'cstream.ini')
cli:add_flag('-x, --dryrun', 'do not store events, just receive them')
cli:add_flag('-d, --debug', 'receiver will run in debug mode')
cli:add_flag('-v, --version', "prints the program's version and exits")

local args = cli:parse_args()
if not args then
  return
end

if args['v'] then
  return print('cstream.lua: version ' .. _VERSION)
end

local dry_run = args['x']
local config = parse_ini(args['f'])

local include = [[
  package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

  require('zhelpers')
  local cmsgpack = require('cmsgpack')
  local yaml     = require('yaml')
  local zmq      = require('lzmq')
  local zthreads = require('lzmq.threads')
  local ztimer   = require('lzmq.timer')
  local zpoller  = require('lzmq.poller')
  local statsd   = require('statsd')({ host='127.0.0.1',
                                       port=8125,
                                       namespace='cstream' })
]]

local init_client = include .. [[
  local CONFIG = ]] .. serpent.block(config, { comment=false }) .. [[
  local FRONTEND = ]] .. ('%q'):format(FRONTEND) .. [[
  local BACKEND = ]]  .. ('%q'):format(BACKEND)  .. [[
  local context = zmq.context()
]]

local init_worker = include .. [[
  local CONFIG = ]] .. serpent.block(config, { comment=false }) .. [[
  local FRONTEND = ]] .. ('%q'):format(FRONTEND) .. [[
  local BACKEND = ]]  .. ('%q'):format(BACKEND)  .. [[
  local context = zthreads.get_parent_ctx()
]]

local client_task = init_client .. [[
  local config = CONFIG['stream'][provider]
  local subscriber, err = context:socket{ zmq.SUB,
                                          subscribe = '',
                                          connect = 'tcp://' ..
                                            config.host .. ':' ..
                                            config.port }

  zassert(subscriber, err)

  local client, err = context:socket{ zmq.DEALER,
                                      connect = FRONTEND }
  zassert(client, err)

  local poller = zpoller.new(2)

  poller:add(subscriber, zmq.POLLIN, function()
    local msg = subscriber:recv()
    local event = { provider=provider, msg=msg, ts=ztimer.absolute_time() }
    client:send(cmsgpack.pack(event))
    statsd:increment('client.' .. provider .. '.events')

    -- print('CLIENT EVENT:\n', yaml.dump(msg))

  end)

  poller:add(client, zmq.POLLIN, function()
    local reply = client:recv_all()

    -- print('REPLY:', yaml.dump(reply))

    local ts = tonumber(reply[#reply])
    statsd:histogram('client.' .. provider .. '.rtt',
                       ztimer.absolute_elapsed(ts))

    -- print('RTT:', ztimer.absolute_delta(ztimer.absolute_time(), ts))

  end)

  poller:start()
]]

local worker_task = init_worker .. [[
  local providers = require('providers')
  local config = CONFIG['worker'][group]

  -- Init storage engine client
  assert(config.engine, 'storage engine name is absent in config file')
  local storage = require('storage.' .. config.engine)

  assert(config.host, 'storage engine host absent in config file')
  assert(config.port, 'storage engine port absent in config file')
  local b = config.bufsize and tonumber(config.bufsize) or 1000
  local h = config.history and tonumber(config.history) or 100
  local w = config.writeconcern and tonumber(config.writeconcern) or 0

  if config.journaled then
    j = (tonumber(config.journaled) == 1)
  else
    j = false
  end

  local storage_parameters = { host=config.host,
                               port=tonumber(config.port),
                               history=h,
                               bufsize=b,
                               writeconcern=w,
                               journaled=j }

  -- print(yaml.dump(storage_parameters))

  if config.user and config.password then
    storage_parameters.user = config.user
    storage_parameters.password = config.password
  end

  local storage_client = storage.new(storage_parameters)

  local worker, err = context:socket{ zmq.DEALER,
                                      connect = BACKEND }
  zassert(worker, err)

  print('Worker', group, 'inited.')

  while true do
    local identity, request = worker:recvx()
    assert(identity and request)

    local timer = zmq.utils.stopwatch():start()

    event = cmsgpack.unpack(request)
    data = providers.parse_msg(event.provider, event.msg)

    -- print('WORKER DATA:\n', yaml.dump(data))
    -- print('WORKER TS  :', event.ts)

    storage_client:save_event(data)

    worker:sendx(identity, tostring(event.ts))

    statsd:increment('worker.' .. group .. '.events')
    statsd:histogram('worker.' .. group .. '.latency', timer:stop())
  end
]]

local main_task = init_client .. [[
  local frontend, err = context:socket{ zmq.ROUTER,
                                        bind = FRONTEND }
  zassert(frontend, err)

  local backend,  err = context:socket{ zmq.DEALER,
                                        bind = BACKEND }
  zassert(backend, err)

  for group, worker in pairs(CONFIG['worker']) do
    if worker.active then
      for i = 1, tonumber(worker.instances) do
        zthreads.run(context, 'local group = ' .. string.format('%q', group) ..
                       '\n' .. ]] .. string.format('%q', worker_task) ..[[):start(true)
        sleep(1)
      end
    end
  end

  zmq.proxy(frontend, backend)
]]

if args['x'] then
   -- dry run, recieve messages and do nothing
   local context = zmq.context()
   local subscriber, err = context:socket{ zmq.SUB,
                                           subscribe = '',
                                           connect = 'tcp://' ..
                                                     config['stream']['rutarget'].host ..
                                              ':' .. config['stream']['rutarget'].port }

   zassert(subscriber, err)
   sleep(1)

   while true do
     local msg = subscriber:recv()
     statsd:increment('client.rutarget.dryrun.events')
   end
end

print('Begin threads initialization...')

-- Launch pool of client threads
for name, stream in pairs(config['stream']) do
  if stream.active then
    zthreads.run(nil, 'local provider = ' .. string.format('%q', name) ..
                   '\n' .. client_task):start(true)
    sleep(1)
  end
end

zthreads.run(nil, main_task):start(true)

print('Initialization is done.')

-- zthreads.join()
while true do
  sleep(5)
end
