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
import utwist

from twisted.internet import endpoints, defer, protocol
from twisted.internet import reactor

from anycall.packet import PacketProtocol
from twisted.python.failure import Failure

class TestPacketProtocol(unittest.TestCase):
    """
    Tests for `PacketProtocol`. This are integration tests that
    use TCP connections.
    """
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        server_factory = PacketFactory()
        client_factory = PacketFactory()
        
        server_endpoint = yield endpoints.TCP4ServerEndpoint(reactor, 0)
        self.listening_port = yield server_endpoint.listen(server_factory)
        self.port = self.listening_port.getHost().port
          
        client_endpoint = endpoints.TCP4ClientEndpoint(reactor, "localhost", self.port)
        self.client_protocol = yield client_endpoint.connect(client_factory)
    
        self.server_protocol = yield server_factory.protocols.get()
        
        yield self.client_protocol.wait_for_connection()
        yield self.server_protocol.wait_for_connection()
    
    @defer.inlineCallbacks
    def twisted_teardown(self):
        self.client_protocol.transport.loseConnection()
        self.server_protocol.transport.loseConnection()
        
        yield self.client_protocol.wait_for_disconnect()
        yield self.server_protocol.wait_for_disconnect()
        yield defer.maybeDeferred(self.listening_port.stopListening)
    
    @utwist.with_reactor
    def test_connect_disconnect(self):
        pass # just testing the setup and teardown of the connection
    
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_client_to_server(self):
        
        self.client_protocol.send_packet("typeA", "Hello World!")
        typename, packet = yield self.server_protocol.read()
        
        self.assertEquals(typename, "typeA")
        self.assertEquals(packet, "Hello World!")

    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_server_to_client(self):
        
        self.server_protocol.send_packet("typeA", "Hello World!")
        typename, packet = yield self.client_protocol.read()
        
        self.assertEquals(typename, "typeA")
        self.assertEquals(packet, "Hello World!")

    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_multiple_messages(self):
        
        self.client_protocol.send_packet("typeA", "Hello")
        self.client_protocol.send_packet("typeB", "World!")
        typename1, packet1 = yield self.server_protocol.read()
        typename2, packet2 = yield self.server_protocol.read()
        
        self.assertEquals(typename1, "typeA")
        self.assertEquals(packet1, "Hello")
        self.assertEquals(typename2, "typeB")
        self.assertEquals(packet2, "World!")
        
    @utwist.with_reactor
    def test_missing_type(self):
        self.assertRaises(ValueError, self.client_protocol.send_packet, "nosuchtype", "Hello")
        
class MockProtocol(PacketProtocol):

    def __init__(self):
        PacketProtocol.__init__(self)
        self.register_type("typeA")
        self.register_type("typeB")
        
        self._packets = defer.DeferredQueue()
        self._connected = defer.Deferred()
        self._disconnected = defer.Deferred()
        
    def wait_for_connection(self):
        d = defer.Deferred()
        self._connected.chainDeferred(d)
        return d
    
    def wait_for_disconnect(self):
        d = defer.Deferred()
        self._disconnected.chainDeferred(d)
        return d
    
    def read(self):
        return self._packets.get()
        

    def connectionMade(self):
        PacketProtocol.connectionMade(self)
        self._connected.callback(None)

    def connectionLost(self, reason=protocol.connectionDone):
        PacketProtocol.connectionLost(self, reason=reason)
        while self._packets.waiting:
            self._packets.put(Failure("Connection closed before packet was received", ValueError))
        if self._packets.pending:
            value = Failure("Received %s unexpected packets" % len(self._packets.pending), ValueError)
        else:
            value = None
        self._disconnected.callback(value)

        
    def on_typeA(self, packet):
        self._packets.put(("typeA", packet))
            
    def on_typeB(self, packet):
        self._packets.put(("typeB", packet))
    
    
class PacketFactory(protocol.Factory):
    protocol = MockProtocol
    
    def __init__(self):
        self.protocols = defer.DeferredQueue()
    
    def buildProtocol(self, addr):
        p = protocol.Factory.buildProtocol(self, addr)
        self.protocols.put(p)
        return p