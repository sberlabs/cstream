# Click stream publisher and subscriber

## Deployment

On the control machine (full install):

    $ git clone git@github.com:sberlabs/cstream.git
    $ cd cstream
    $ ansible-playbook -i hosts -l prod deploy.yml

Click stream subscriber daemon will be started automatically.

To start/stop/restart click stream subscriber daemon, use:

    $ sudo supervisorctl
    supervisor> stop cstream
    supervisor> start cstream

## Subscriber daemon

    $ ./cstream_sub.lua --help
    Usage: cstream_sub.lua [OPTIONS]  ADDR  PORT
    
    ARGUMENTS:
        ADDR                event stream publisher address (required)
        PORT                event stream publisher base port (required)
    
    OPTIONS:
        -s, --storage=TYPE  storage engine type: [redis|mongodb] (default:
                            redis)
        -p, --provider=NAME message parser type: [rutarget|testprovider]
                            (default: rutarget)
        -x, --dryrun        do not store clicks, just receive them
        -y, --sync          send sync request to publisher before subscribing
        -d, --debug         receiver will run in debug mode
        -v, --version       prints the program's version and exits

### Subscriber supervisord config

See config/cstream.conf

## Publisher (used mostly for testing purposes)

    $ ./cstream_pub.lua --help
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
