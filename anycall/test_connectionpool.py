# Copyright (c) 2014 Stefan C. Mueller

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import unittest
import socket

import utwist
from twisted.internet import defer, endpoints, reactor

from anycall import connectionpool
from twisted.python.failure import Failure


class TestConnectionPool(unittest.TestCase):
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        
        #import sys
        #from twisted.python import log
        #log.startLogging(sys.stdout)
        
        host = socket.getfqdn()
        server_endpointA = endpoints.TCP4ServerEndpoint(reactor, 50000)
        server_endpointB = endpoints.TCP4ServerEndpoint(reactor, 50001)
        self.poolA = MockPool(server_endpointA, host + ":50000")
        self.poolB = MockPool(server_endpointB, host + ":50001")
        self.poolA.register_type("msg")
        self.poolB.register_type("msg")
        yield self.poolA.open()
        yield self.poolB.open()
        
    @defer.inlineCallbacks
    def twisted_teardown(self):
        yield self.poolA.close()
        yield self.poolB.close()
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_one(self):
        yield self.poolA.send(self.poolB.ownid, "msg", "Hello World!")
        peer, typename, msg = yield self.poolB.packets.get()
        self.assertEqual(peer, self.poolA.ownid)
        self.assertEqual(typename, "msg")
        self.assertEqual(msg, "Hello World!")
    
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_two(self):
        yield self.poolA.send(self.poolB.ownid, "msg", "Hello World!")
        peer, typename, msg = yield self.poolB.packets.get()
        self.assertEqual(peer, self.poolA.ownid)
        self.assertEqual(typename, "msg")
        self.assertEqual(msg, "Hello World!")
    
        yield self.poolB.send(self.poolA.ownid, "msg", "Hello World!")
        peer, typename, msg = yield self.poolA.packets.get()
        self.assertEqual(peer, self.poolB.ownid)
        self.assertEqual(typename, "msg")
        self.assertEqual(msg, "Hello World!")
        
class MockPool(connectionpool.ConnectionPool):
    
    def __init__(self, stream_server_endpoint, ownid):
        connectionpool.ConnectionPool.__init__(self, stream_server_endpoint, self.make_client_endpoint, ownid)
        self.packets = defer.DeferredQueue()
        
    def open(self):
        return connectionpool.ConnectionPool.open(self, self.packet_received)

    def packet_received(self, peer, typename, data):
        self.packets.put((peer, typename, data))
    
    def make_client_endpoint(self, peer):
        host, port = peer.split(":")
        return endpoints.TCP4ClientEndpoint(reactor, host, int(port))
        
    @defer.inlineCallbacks
    def close(self):
        yield connectionpool.ConnectionPool.close(self)
        
        while self.packets.waiting:
            self.packets.put(Failure("Connection closed before packet was received", ValueError))
        if self.packets.pending:
            value = Failure("Received %s unexpected packets" % len(self._packets.pending), ValueError)
        else:
            value = None
            
        yield defer.returnValue(value)