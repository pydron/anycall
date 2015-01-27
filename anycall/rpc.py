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

import logging
import bidict
import uuid
import urlparse
import pickle
import socket

import twistit

logger = logging.getLogger(__name__)

from twisted.internet import defer, task, reactor, endpoints
from twisted.python import failure

from anycall import connectionpool


def create_tcp_rpc_system(hostname=None, port=0, ping_interval=1, ping_timeout=0.5):
    """
    Creates a TCP based :class:`RPCSystem`.
    """
    
    def ownid_factory(listeningport):
        port = listeningport.getHost().port
        return "%s:%s" %(hostname, port)

    def make_client_endpoint(peer):
        host, port = peer.split(":")
        if host == socket.getfqdn():
            host = "localhost"
        return endpoints.TCP4ClientEndpoint(reactor, host, int(port))
    
    if hostname is None:
        hostname = socket.getfqdn()

    server_endpointA = endpoints.TCP4ServerEndpoint(reactor, port)
    pool = connectionpool.ConnectionPool(server_endpointA, make_client_endpoint, ownid_factory)
    return RPCSystem(pool, ping_interval=ping_interval, ping_timeout=ping_timeout)


class RPCSystem(object):
    
    _MESSAGE_TYPE = "RPC"
    
    _PING = uuid.uuid5(uuid.NAMESPACE_URL, "ping")
    
    def __init__(self, connectionpool, ping_interval = 5*60, ping_timeout = 60):
        """
        :param connectionpool: Messaging system to use for low-level communication.
        
        :param ping_interval: Every `ping_interval` seconds we send for each unfinished
           call a message to our peers asking conformation that they are still working
           on it. If the peer has somehow 'lost' the call, or if we don't get a reply
           within `ping_timeout` seconds, the call will fail.
           
           This protects against unnoticed loss of connection, or if the remote process
           dies unexpectantly. In such cases the call might otherwise hang forever .
           
         :param ping_interval: See `ping_interval`-
        """
        self._connectionpool = connectionpool
        self._connectionpool.register_type(self._MESSAGE_TYPE)
        
        #: Local functions that may be invoked from remote.
        #: maps `functionid <-> callable`
        self._functions = bidict.bidict()
        
        #: Calls made from remote to here that are currently in progress.
        #: Maps `(peerid, callid)` -> `Deferred`.
        self._remote_to_local = {}
        
        #: Calls made from here to remote functions.
        #: Maps `(peerid, callid)` -> `Deferred`.
        self._local_to_remote = {}

        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._ping_loop = task.LoopingCall(self._ping_loop_iteration)
        self._ping_current_iteration = None # self._ping_loop.cancel() won't cancel an ongoing call, so we use this deferred.
        
        self._functions[self._PING] = self._ping

    def open(self):
        """
        Opens the port.
        
        :returns: Deferred that callbacks when we are ready to make and receive calls.
        """
        logging.debug("Opening rpc system")
        d = self._connectionpool.open(self._packet_received)
        
        def opened(_):
            logging.debug("Starting ping loop")
            self._ping_loop.start(self._ping_interval, now=False)
        
        d.addCallback(opened)
        return d
    
    def close(self):
        """
        Stop listing for new connections and close all open connections.
        
        :returns: Deferred that calls back once everything is closed.
        """
        logging.debug("Closing rpc system. Stopping ping loop")
        self._ping_loop.stop()
        if self._ping_current_iteration:
            self._ping_current_iteration.cancel()
        return self._connectionpool.close()

    def get_function_url(self, function):
        """
        Registers the given callable in the system (if it isn't already)
        and returns the URL that can be used to invoke the given function from remote.
        """
        logging.debug("get_function_url(%s)" % repr(function))
        if function in ~self._functions:
            functionid = self._functions[:function]
        else:
            functionid = uuid.uuid1()
            self._functions[functionid] = function
        return "anycall://%s/functions/%s" % (self._connectionpool.ownid, functionid.hex)


    def create_function_stub(self, url):
        """
        Create a callable that will invoke the given remote function.
        
        The stub will return a deferred even if the remote function does not.
        """
        logging.debug("create_function_stub(%s)" % repr(url))
        parseresult = urlparse.urlparse(url)
        scheme = parseresult.scheme
        path = parseresult.path.split("/")
        if scheme != "anycall":
            raise ValueError("Not an anycall URL: %s" % repr(url))
        if len(path) != 3 or path[0] != "" or path[1] != "functions":
            raise ValueError("Not an URL for a remote function: %s" % repr(url))
        try:
            functionid = uuid.UUID(path[2])
        except ValueError:
            raise ValueError("Not a valid URL for a remote function: %s" % repr(url))
        
        return _RPCFunctionStub(parseresult.netloc, functionid, self)
    
    
    def _send(self, peer, obj):
        logger.debug("Sending to %s: %s" % (peer, repr(obj)))
        return self._connectionpool.send(peer, self._MESSAGE_TYPE, pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))
    
    def _packet_received(self, peerid, typename, data):
        try:
            if typename != self._MESSAGE_TYPE:
                raise ValueError("Received unexpected packet type:%s" % typename)
            
            obj = pickle.loads(data)
    
            logger.debug("Received from %s: %s" % (peerid, repr(obj)))
            
            if isinstance(obj, _Call):
                self._Call_received(peerid, obj)
            elif isinstance(obj, _CallReturn):
                self._CallReturn_received(peerid, obj)
            elif isinstance(obj, _CallFail):
                self._CallFail_received(peerid, obj)
            elif isinstance(obj, _CallCancel):
                self._CallCancel_received(peerid, obj)
            else:
                raise ValueError("Received unknown object type")
        except:
            logger.exception("error while receiving package")

    def _Call_received(self, peerid, obj):
        if obj.functionid not in self._functions:
            raise ValueError("Call for unregistered function.")
        
        func = self._functions[obj.functionid]
        
        logger.debug("Invoking %s for peer %s." % (repr(func), peerid))
        d = defer.maybeDeferred(func, *obj.args, **obj.kwargs)
        
        self._remote_to_local[(peerid, obj.callid)] = d
        
        def on_success(retval):
            if (peerid, obj.callid) in self._remote_to_local:
                del self._remote_to_local[(peerid, obj.callid)]
                return self._send(peerid, _CallReturn(obj.callid, retval))
            
        def on_fail(failure):
            if (peerid, obj.callid) in self._remote_to_local:
                del self._remote_to_local[(peerid, obj.callid)]
                return self._send(peerid, _CallFail(obj.callid, failure))
        
        d.addCallbacks(on_success, on_fail)
        
        def uncought(failure):
            logger.error(str(failure))
            
        d.addErrback(uncought)

    def _CallReturn_received(self, peerid, obj):
        try:
            d = self._local_to_remote.pop((peerid, obj.callid))
        except KeyError:
            raise ValueError("Received return value for non-existent call.")
        d.callback(obj.retval)
        
    def _CallFail_received(self, peerid, obj):
        try:
            d = self._local_to_remote.pop((peerid, obj.callid))
        except KeyError:
            raise ValueError("Received failure for non-existent call.")
        d.errback(obj.failure)
        
    def _CallCancel_received(self, peerid, obj):
        try:
            d = self._remote_to_local.pop((peerid, obj.callid))
            d.cancel()
        except KeyError:
            # We have sent the result already.
            pass
        
    def _invoke_function(self, peerid, functionid, args, kwargs):
        def canceller(d):
            if (peerid, callid) in self._local_to_remote:
                
                def uncought(failure):
                    logger.error(str(failure))
                
                d = self._send(peerid, _CallCancel(callid))
                d.addErrback(uncought)
        
        callid = uuid.uuid1()
        call = _Call(callid, functionid, args, kwargs)
        
        # We want to have `_local_to_remote` set before
        # we call `_send`. Just in case we get an answer
        # before the deferred we get from `_send` reports
        # success. We will not pass this deferred
        # on if the send operation has failed.
        d = defer.Deferred(canceller)
        self._local_to_remote[(peerid, callid)] = d
        
        d_send = self._send(peerid, call)
        
        def send_success(_):
            return d
        
        def send_failed(failure):
            del self._local_to_remote[(peerid, callid)]
            return failure
        
        d_send.addCallbacks(send_success, send_failed)
        return d_send

    def _ping_loop_iteration(self):
        """
        Called every `ping_interval` seconds.
        Invokes `_ping()` remotely for every ongoing call.
        """
        
        deferredList = []
        
        for peerid, callid in list(self._local_to_remote):
            
            if (peerid, callid) not in self._local_to_remote:
                continue # call finished in the meantime
            
            logger.debug("sending ping")
            d = self._invoke_function(peerid, self._PING, (self._connectionpool.ownid, callid), {})
            twistit.timeout_deferred(d, self._ping_timeout, "Lost communication to peer during call.")
            
            def failed(failure):
                if (peerid, callid) in self._local_to_remote:
                    d = self._local_to_remote.pop((peerid, callid))
                    d.errback(failure)
                    
            def success(value):
                logger.debug("received pong")
                return value
                    
            d.addCallbacks(success, failed)
            deferredList.append(d)
   
        d = defer.DeferredList(deferredList)
            
        def done(_):
            self._ping_current_iteration = None
        self._ping_current_iteration = d
        d.addBoth(done)
        return d
    
    def _ping(self, peerid, callid):
        """
        Called from remote to ask if a call made to here is still in progress.
        """
        if not (peerid, callid) in self._remote_to_local:
            raise ValueError("No remote call %s from %s." % (callid, peerid))

class _RPCFunctionStub(object):
    def __init__(self, peerid, functionid, rpcsystem):
        self.peerid = peerid
        self.functionid = functionid
        self.rpcsystem = rpcsystem
    
    def __call__(self, *args, **kwargs):
        return self.rpcsystem._invoke_function(self.peerid, self.functionid, args, kwargs)
    
    def __repr__(self):
        return "RPCStub(%s)" % repr(self.url)
    
    def __str__(self):
        return repr(self)
    
    def __eq__(self, other):
        return self.peerid == other.peerid and self.functionid == other.functionid
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        return hash(self.peerid) + hash(self.functionid)


class _Call(object):
    def __init__(self, callid, functionid, args, kwargs):
        self.callid = callid
        self.functionid = functionid
        self.args = args
        self.kwargs = kwargs
    def __repr__(self):
        return "_Call(%s, %s, %s, %s)" %(repr(self.callid), repr(self.functionid), repr(self.args), repr(self.kwargs))
        
class _CallReturn(object):
    def __init__(self, callid, retval):
        self.callid = callid
        self.retval = retval
    def __repr__(self):
        return "_CallReturn(%s, %s)" %(repr(self.callid), repr(self.retval))
        
class _CallFail(object):
    def __init__(self, callid, failure):
        self.callid = callid
        self.failure = failure
    def __repr__(self):
        return "_CallFail(%s, %s)" %(repr(self.callid), repr(self.failure))
    
class _CallCancel(object):
    def __init__(self, callid):
        self.callid = callid
    def __repr__(self):
        return "_CallCancel(%s)" %(repr(self.callid))
    
