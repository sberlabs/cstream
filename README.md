# Click stream publisher and subscriber

## Publisher (used mostly for testing purposes)

<pre>
Usage: cstream_pub.lua [OPTIONS]  PORT

ARGUMENTS:
  PORT                   base port for click stream publishing
                         (required)

OPTIONS:
  -i, --inputfile=FILE   read click stream from a FILE
  -u, --udp=UDP_PORT     receive click stream on
                         udp://localhost:UDP_PORT
  -f, --format=INI_FILE  tsv format definition file name (default:
                         cstream.ini)
  -s, --subscribers=SUBS the number of expected subscribers (default: 1)
  -d, --debug            receiver will run in debug mode
  -v, --version          prints the program's version and exits
</pre>

## Subscriber

<pre>
Usage: cstream_sub.lua [OPTIONS]  ADDR  PORT

ARGUMENTS:
  ADDR                click stream publisher address (required)
  PORT                click stream publisher base port (required)

OPTIONS:
  -p, --provider=NAME provider's module name to import data into redis
                      (default: rutarget)
  -s, --sync          send sync request to publisher before subscribing
  -d, --debug         receiver will run in debug mode
  -v, --version       prints the program's version and exits
</pre>

### Provider-specific subscriber modules

All specific code related to incoming click stream messages (incl. storing them into redis) is encapsulated in provider-specific modules. See 'rutarget.lua' as an example.
