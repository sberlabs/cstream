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

-- common tasks code

local init_thread = [[
  package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

  require('zhelpers')
  local cmsgpack = require('cmsgpack')
  local yaml = require('yaml')
  local zmq = require('lzmq')
  local statsd  = require('statsd')({ host='127.0.0.1',
                                      port=8125,
                                      namespace='cstream' })
  local context = zmq.context()

  local FRONTEND = ]] .. ('%q'):format(FRONTEND) .. [[

  local BACKEND = ]]  .. ('%q'):format(BACKEND)  .. [[

  local CONFIG = ]] .. serpent.block(config, { comment=false }) .. [[

]]

-- initialize client tasks' code

local client_tasks = {}
local num_clients = 0

for k, v in pairs(config['stream']) do
  if v.active == '1' then
    num_clients = num_clients + 1
    client_tasks[k] = init_thread .. [[

      local dry_run  = ]] .. tostring(args['x']) .. [[

      local provider = ']] .. k .. [['

      local subscriber, err = context:socket{ zmq.SUB,
                                              subscribe = '',
                                              connect = 'tcp://'
                                              ..']] .. v.host .. [[' .. ':'
                                              ..']] .. v.port .. [[' }
      zassert(subscriber, err)

      local client, err = context:socket{ zmq.REQ,
                                          connect = FRONTEND }
      zassert(client, err)

      sleep(1)

      while true do
        local msg = subscriber:recv()
        local event = { provider=provider, msg=msg }

        if not dry_run then
          client:send(cmsgpack.pack(event))
          local reply = client:recv()
        end

        statsd:increment('client.' .. provider .. '.events')
      end
    ]]
  end
end

-- initialize worker tasks' code

local worker_tasks = {}
local num_workers = 0

for k, v in pairs(config['worker']) do
  num_workers = num_workers + tonumber(v.instances)
  worker_tasks[k] = init_thread .. [[

    local providers = require('providers')
    local group = ']] .. k .. [['
    local config = CONFIG['worker'][group]

    -- Init storage engine client
    local storage = require('storage.' .. config.engine)
    local storage_parameters = { host=config.host,
                                 port=config.port,
                                 history=config.history }

    if config.user and config.password then
      storage_parameters.user = config.user
      storage_parameters.password = config.password
    end

    local storage_client = storage.new(storage_parameters)
    local worker, err = context:socket{ zmq.REQ,
                                        connect = BACKEND }
    zassert(worker, err)

    sleep(1)

    -- Tell broker we're ready for work
    worker:send("READY")

    while true do
      local identity, empty, request = worker:recvx()
      assert(empty == "")

      local timer = zmq.utils.stopwatch():start()
      event = cmsgpack.unpack(request)
      data = providers.parse_msg(event.provider, event.msg)
      storage_client:save_event(data)

      worker:sendx(identity, "", "OK")

      statsd:increment('worker.' .. group .. '.events')
      statsd:histogram('worker.' .. group .. '.latency', timer:stop())
    end
  ]]
end

-- main task (request broker)

local context = zmq.context()

local frontend, err = context:socket{ zmq.ROUTER,
                                      bind = FRONTEND }
zassert(frontend, err)

local backend,  err = context:socket{ zmq.ROUTER,
                                      bind = BACKEND }
zassert(backend, err)

print('Launching worker threads...')

-- Launch pool of worker threads
for k, worker_task in pairs(worker_tasks) do
  for i = 1, tonumber(config['worker'][k].instances) do
    zthreads.run(context, worker_task):start(true)
    sleep(1)
  end
end

print('Launching client threads...')

-- Launch pool of client threads
for k, client_task in pairs(client_tasks) do
  zthreads.run(context, client_task):start(true)
  sleep(1)
end

local worker_queue = {}

local poller = zpoller.new(2)

local function frontend_cb()
  assert (#worker_queue > 0)

  -- Now get next client request, route to last-used worker
  -- Client request is [identity][empty][request]
  local msg = frontend:recv_all()
  assert(msg[2] == "")

  local worker_id = tremove(worker_queue, 1)
  backend:sendx_more(worker_id, "")
  backend:send_all(msg)
end

-- Handle worker activity on backend
local function backend_cb()
  assert (#worker_queue < num_workers)

  -- Queue worker identity for load-balancing
  local worker_id = backend:recv()
  tinsert(worker_queue, worker_id)

  -- Second frame is empty
  local empty = backend:recv()
  assert(empty == "")

  -- Third frame is READY or else a client reply identity
  local client_id = backend:recv()

  -- If client reply, send rest back to frontend
  if client_id ~= "READY" then
    empty = backend:recv()
    assert(empty == "")

    local reply = backend:recv()
    frontend:send_all{client_id, "", reply}
  end
end

poller:add(backend, zmq.POLLIN, function()
  local n = #worker_queue
  backend_cb()
  if (n == 0) and (#worker_queue > 0) then
    poller:add(frontend, zmq.POLLIN, function()
      frontend_cb()
      if #worker_queue == 0 then poller:remove(frontend) end
    end)
  end
end)

print('Running with ' .. num_clients .. ' client and ' ..
        num_workers .. ' worker threads.')

poller:start()
