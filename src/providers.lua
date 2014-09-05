package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local providers = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Provider-specific click stream data parsers'
}

local cjson = require('cjson.safe')

providers.parse_msg = function(provider, data)
  if provider == 'rutarget' then
    return cjson.decode(data)
  elseif provider == 'testprovider' then
    local t = cjson.decode(data)
    t['id'] = t['sberlabspx']
    t['url'] = 'http://www.sberlabs-oh-yeah.com/some/url?with=1&parameters'
    t['ts'] = t['msec']
    t['msec'] = nil
    t['sberlabspx'] = nil
    return t
  end
  return nil
end

return providers

