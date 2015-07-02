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

import struct
import binascii
import logging
import bytequeue

logger = logging.getLogger(__name__)

from twisted.internet import protocol
from twisted.python import log

class PacketProtocol(protocol.Protocol):
    """
    Implements a packet protocol on top of a stream protocol.
    
    Each packet has a type and a content. Both are strings.
    The packet types have to be registered before the connection is made. Only
    registered types can be used for sending and receiving.
    
    The type is not sent as a string. Instead we send the hash value of it.
    This can potentially lead to collisions which cause the registration to fail.
    Since the types are usally hard-coded this is not a problem in pratice.
    
    This class is designed to be overwritten. If a packet is received, its content
    is passed to `self.on_[typename](packet)`. If there is no such method, an error
    is logged and the connection is closed. 
    """
    
    def __init__(self):
        self._unprocessed_data = None
        self._header = struct.Struct(">II")
        self._type_register = {}
        
    def register_type(self, typename):
        """
        Registers a type name so that it may be used to send and receive packages.
        
        :param typename: Name of the packet type. A method with the same name and a
            "on_" prefix should be added to handle incomming packets.
            
        :raises ValueError: If there is a hash code collision.
        """ 
        typekey = typehash(typename)
        if typekey in self._type_register:
            raise ValueError("Type name collision. Type %s has the same hash." % repr(self._type_register[typekey]))
        self._type_register[typekey] = typename
    
    def connectionMade(self):
        self._unprocessed_data = bytequeue.ByteQueue()
        
    def connectionLost(self, reason=protocol.connectionDone):
        self._unprocessed_data = None
        
    def dataReceived(self, data):
        """
        Do not overwrite this method. Instead implement `on_...` methods for the
        registered typenames to handle incomming packets.
        """
        
        self._unprocessed_data.enqueue(data)
        
        
        while True:
            if len(self._unprocessed_data) < self._header.size:
                return # not yet enough data
            
            hdr_data = self._unprocessed_data.peek(self._header.size)
            packet_length, typekey = self._header.unpack(hdr_data)
            total_length = self._header.size + packet_length
            
            if len(self._unprocessed_data) < total_length:
                return # not yet enough data
            
            self._unprocessed_data.drop(self._header.size)
            packet = self._unprocessed_data.dequeue(packet_length)
            
            self._start_receive = None
            
            typename = self._type_register.get(typekey, None)
            if typename is None:
                self.on_unregistered_type(typekey, packet)
            else:
                self.packet_received(typename, packet)

                
    def send_packet(self, typename, packet):
        """
        Send a packet.
        
        :param typename: A previously registered typename.
        
        :param packet: String with the content of the packet.
        """
        typekey = typehash(typename)
        if typename != self._type_register.get(typekey, None):
            raise ValueError("Cannot send packet with unregistered type %s." % repr(typename))
        
        hdr = self._header.pack(len(packet), typekey)
        self.transport.writeSequence([hdr, packet])
        
    def packet_received(self, typename, packet):
        raise ValueError("abstract")
        
        
    def on_unregistered_type(self, typekey, packet):
        """
        Invoked if a packet with an unregistered type was received.
        
        Default behaviour is to log and close the connection.
        """
        log.msg("Missing handler for typekey %s in %s. Closing connection." % (typekey, type(self).__name__))
        self.transport.loseConnection()

def typehash(typename):
    """
    Transforms a typename to a number.
    """
    return binascii.crc32(typename) & 0xffffffff