import os, sys, time, signal

__all__ = ['root', 'client']

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

# add gen-py and cassandra directories to sys.path
L = os.path.abspath(__file__).split(os.path.sep)[:-2]
root = os.path.sep.join(L)
_ipath = os.path.join(root, 'interface', 'gen-py')
sys.path.append(_ipath)
sys.path.append(os.path.join(_ipath, 'com', 'facebook', 'infrastructure', 'service'))
import Cassandra

host, port = '127.0.0.1', 9160
def get_client():
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client

client = get_client()


class CassandraTester(object):
    # leave this True unless you are manually starting a server and then
    # running only a single test against it; tests assume they start against an empty db.
    runserver = True 

    def setUp(self):
        if self.runserver:
            # clean out old stuff
            import shutil
            # todo get directories from conf/storage-conf.xml
            for dirname in ['system', 'data', 'commitlog']:
                try:
                    shutil.rmtree('/var/cassandra/' + dirname)
                except OSError:
                    pass
                os.mkdir('/var/cassandra/' + dirname)

            # start the server
            import subprocess as sp
            os.chdir(root)
            args = open(os.path.join('bin', 'start-server')).read().strip().split()
            self.server_process = sp.Popen(args, stderr=sp.PIPE, stdout=sp.PIPE)
            time.sleep(0.1)

            # connect to it, with a timeout in case something went wrong
            start = time.time()
            while time.time() < start + 20:
                try:
                    client.transport.open()
                except:
                    time.sleep(0.1)
                else:
                    break
            else:
                os.kill(self.server_process.pid, signal.SIGKILL) # just in case
                print self.server_process.stdout.read()
                print self.server_process.stderr.read()
                print "Couldn't connect to server; aborting regression test"
                sys.exit()
        else:
            client.transport.open()

    def tearDown(self):
        if self.runserver:
            client.transport.close()
            os.kill(self.server_process.pid, signal.SIGTERM)
            start = time.time()
            while time.time() < start + 5:
                time.sleep(0.1)
                if self.server_process.poll() is not None:
                    break
            if time.time() > start + 5:
                os.kill(self.server_process.pid, signal.SIGKILL)
            time.sleep(0.5)
            assert self.server_process.poll() is not None
