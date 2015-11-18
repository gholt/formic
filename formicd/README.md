## You need to ether be running oort with the redis protocol, or run a redis server instance.

<pre>
$ ./formic -h
Usage of ./formic:
  -cert_file="server.crt": The TLS cert file
  -key_file="server.key": The TLS key file
  -oorthost="127.0.0.1:6379": host:port to use when connecting to oort
  -port=8443: The server port
  -tls=true: Connection uses TLS if true, else plain TCP
</pre>

*To run*

<pre>
go build . && ./formic
</pre>

## formicd command line options(with defaults):

Option | Description
------ | -----------
-cert_file string | The TLS cert file (default "/etc/oort/server.crt")      
-key_file string | The TLS key file (default "/etc/oort/server.key")
-oorthost string | host:port to use when connecting to oort (default "127.0.0.1:6379")
-port int |          The server port (default 8443)
-tls      |         Connection uses TLS if true, else plain TCP (default true)


## To run as a deamon with systemd:

<pre>
cp -av packaging/root/usr/share/formicd/systemd/formicd.service /lib/systemd/system
</pre>

To override any defaults add the new config options into the /etc/default/formicd file:
* FORMICD_TLS
* FORMICD_OORT_HOST
* FORMICD_PORT
* FORMICD_CERT_FILE
* FORMICD_KEY_FILE

*Example:*

<pre>
echo 'FORMICD_PORT=8444' > /etc/default/formicd
</pre>
