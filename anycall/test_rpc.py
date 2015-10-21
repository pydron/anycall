# Copyright (C) 2014 Stefan C. Mueller

import unittest
import pickle
import cPickle
import StringIO as stringio

import utwist
from twisted.internet import defer, reactor

from anycall import rpc
from anycall.rpc import RPCSystem


class TestRPC(unittest.TestCase):
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        self.rpcA = rpc.create_tcp_rpc_system(port_range=[50000])
        self.rpcB = rpc.create_tcp_rpc_system(port_range=[50001])
        
        yield self.rpcA.open()
        yield self.rpcB.open()
        
    @defer.inlineCallbacks
    def twisted_teardown(self):
        yield self.rpcA.close()
        yield self.rpcB.close()
        RPCSystem.default = None
    
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
        

        # Attempt to fix a race condition 
        # (observed on linux and windows, but it rarely happens, so
        #  others might be affected too.).
        #
        # The backend of twisted is using `socket` API
        # for DNS lookups. This API is blocking, so twisted
        # calls it in a thread. It uses a `reactor.callLater()`
        # to timeout the operation if the thread does not complete
        # in time. If it does, the thread will cancel the `callLater`.
        #
        # This test cancels the send operation and finishes while
        # this thread is still on-going and before the timeout happens.
        # `utwist` then cleans up the reactor which cancels the
        # `callLater`. When the thread finishes a bit later it 
        # tries to cancel the same `callLater`, causing an exception.
        #
        # there is a bug report for this:
        # http://twistedmatrix.com/trac/ticket/2579
        #
        # This is a combination of cancelling the send operation and
        # the cleanup done by `utwist`.
        #
        # We just wait a bit, to give the thread time to finish.
        
        def delay(result):
            d = defer.Deferred()
            reactor.callLater(1, d.callback, result)  # @UndefinedVariable
            return d
        d.addBoth(delay)
            
        
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

        reactor.callLater(2, slow.callback, "Hello World!")  # @UndefinedVariable
        
        actual = yield myfunc_stub()
        self.assertEqual("Hello World!", actual)
        
    @utwist.with_reactor
    def test_pickle_no_default(self):
        slow = defer.Deferred()
        
        def myfunc():
            return slow
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcA.create_function_stub(myfunc_url)
        
        stringfile = stringio.StringIO()
        pickler = pickle.Pickler(stringfile, protocol=pickle.HIGHEST_PROTOCOL)
        pickler.dump(myfunc_stub)
        
        stringfile = stringio.StringIO(stringfile.getvalue())
        pickler = pickle.Unpickler(stringfile)
        
        self.assertRaises(ValueError, pickler.load)
        
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_pickle(self):
        slow = defer.Deferred()
        
        def myfunc():
            return slow
        
        RPCSystem.default = self.rpcB
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcA.create_function_stub(myfunc_url)
        
        stringfile = stringio.StringIO()
        pickler = pickle.Pickler(stringfile, protocol=pickle.HIGHEST_PROTOCOL)
        pickler.dump(myfunc_stub)
        
        stringfile = stringio.StringIO(stringfile.getvalue())
        pickler = pickle.Unpickler(stringfile)
        
        myfunc_stub_loaded = pickler.load()
        
        reactor.callLater(2, slow.callback, "Hello World!")  # @UndefinedVariable
        
        actual = yield myfunc_stub_loaded()
        self.assertEqual("Hello World!", actual)
        
        
    @utwist.with_reactor
    def test_cpickle_no_default(self):
        slow = defer.Deferred()
        
        def myfunc():
            return slow
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcA.create_function_stub(myfunc_url)
        
        stringfile = stringio.StringIO()
        pickler = cPickle.Pickler(stringfile, protocol=cPickle.HIGHEST_PROTOCOL)
        pickler.dump(myfunc_stub)
        
        stringfile = stringio.StringIO(stringfile.getvalue())
        pickler = cPickle.Unpickler(stringfile)
        
        self.assertRaises(ValueError, pickler.load)
        
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_cpickle(self):
        slow = defer.Deferred()
        
        def myfunc():
            return slow
        
        RPCSystem.default = self.rpcB
        
        myfunc_url = self.rpcA.get_function_url(myfunc)
        myfunc_stub = self.rpcA.create_function_stub(myfunc_url)
        
        stringfile = stringio.StringIO()
        pickler = cPickle.Pickler(stringfile, protocol=cPickle.HIGHEST_PROTOCOL)
        pickler.dump(myfunc_stub)
        
        stringfile = stringio.StringIO(stringfile.getvalue())
        pickler = cPickle.Unpickler(stringfile)
        
        myfunc_stub_loaded = pickler.load()
        
        reactor.callLater(2, slow.callback, "Hello World!")  # @UndefinedVariable
        
        actual = yield myfunc_stub_loaded()
        self.assertEqual("Hello World!", actual)