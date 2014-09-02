package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local rutarget = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Click stream redis importer module for RuTarget'
}

local cjson = require('cjson.safe')

rutarget.parse_message = function(s)
  return cjson.decode(s)
end

return rutarget

