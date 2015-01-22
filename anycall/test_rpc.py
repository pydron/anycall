# Copyright (C) 2014 Stefan C. Mueller

import unittest

import utwist
from twisted.internet import defer, reactor

from anycall import rpc


class TestRPC(unittest.TestCase):
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        self.rpcA = rpc.create_tcp_rpc_system(50000)
        self.rpcB = rpc.create_tcp_rpc_system(50001)
        
        yield self.rpcA.open()
        yield self.rpcB.open()
        
    @defer.inlineCallbacks
    def twisted_teardown(self):
        yield self.rpcA.close()
        yield self.rpcB.close()
    
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_simple_call(self):
        
        def myfunc():
            return "Hello World!"
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        actual = yield myfunc_stub()
        self.assertEqual("Hello World!", actual)
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_args(self):
        
        def myfunc(entitiy):
            return "Hello %s!" % entitiy
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        actual = yield myfunc_stub("World")
        self.assertEqual("Hello World!", actual)
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_kwargs(self):
        
        def myfunc(entitiy):
            return "Hello %s!" % entitiy
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        actual = yield myfunc_stub(entitiy="World")
        self.assertEqual("Hello World!", actual)
    
    
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_long_call(self):
        
        d_myfunc = defer.Deferred()
        
        def myfunc():
            return d_myfunc
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        d = myfunc_stub()
        d_myfunc.callback("Hello World!")
        actual = yield d
        self.assertEqual("Hello World!", actual)   
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_cancel_caller(self):
        
        def myfunc():
            defer.Deferred()
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        d = myfunc_stub()
        d.cancel()

        def on_sucess(failure):
            raise ValueError("expected cancel")
        def on_fail(failure):
            return None
        
        d.addCallbacks(on_sucess, on_fail)
        yield d


    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_cancel_callee(self):
        
        was_cancelled = defer.Deferred()
        inner_called = defer.Deferred()
        inner_result = defer.Deferred(lambda _:was_cancelled.callback(None))
        
        def myfunc():
            inner_called.callback(None)
            return inner_result
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        d = myfunc_stub()
        
        # we got to wait til we actually made the call.
        # otherwise we might just cancel the connection process.
        yield inner_called
        
        d.cancel()
        d.addErrback(lambda _:None)
        yield d
        yield was_cancelled
        
    
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_ping(self):        
        
        slow = defer.Deferred()
        
        def myfunc():
            return slow
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcB.create_function_stub(myfunc_url)

        reactor.callLater(2, slow.callback, "Hello World!")
        
        actual = yield myfunc_stub()
        self.assertEqual("Hello World!", actual)