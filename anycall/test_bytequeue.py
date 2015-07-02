'''
Created on 02.07.2015

@author: stefan
'''
import unittest
from anycall import bytequeue


class TestByteQueue(unittest.TestCase):

    def setUp(self):
        self.target = bytequeue.ByteQueue()

    def test_False(self):
        self.assertFalse(self.target)

    def test_True(self):
        self.target.enqueue("x")
        self.assertTrue(self.target)
        
    def test_len_zero(self):
        self.assertEqual(0, len(self.target))
        
    def test_len_2(self):
        self.target.enqueue("ab")
        self.assertEqual(2, len(self.target))
        
    def test_len_2_3(self):
        self.target.enqueue("ab")
        self.target.enqueue("cde")
        self.assertEqual(5, len(self.target))
        
    def test_dequeue_part(self):
        self.target.enqueue("123")
        self.assertEqual("123", self.target.dequeue(3))
        
    def test_dequeue_partial(self):
        self.target.enqueue("123")
        self.assertEqual("12", self.target.dequeue(2))
        self.assertEqual("3", self.target.dequeue(1))
        
    def test_dequeue_two_parts(self):
        self.target.enqueue("123")
        self.target.enqueue("456")
        self.assertEqual("123", self.target.dequeue(3))
        self.assertEqual("456", self.target.dequeue(3))
        
    def test_dequeue_overlap(self):
        self.target.enqueue("123")
        self.target.enqueue("456")
        self.assertEqual("1234", self.target.dequeue(4))
        self.assertEqual("56", self.target.dequeue(2))
        
    def test_peek_part(self):
        self.target.enqueue("123")
        self.assertEqual("123", self.target.peek(3))
        self.assertEqual("123", self.target.peek(3))
        
    def test_peek_partial(self):
        self.target.enqueue("123")
        self.assertEqual("12", self.target.peek(2))
        self.assertEqual("1", self.target.peek(1))
        
    def test_peek_overlap(self):
        self.target.enqueue("123")
        self.target.enqueue("456")
        self.assertEqual("1234", self.target.peek(4))
        self.assertEqual("1234", self.target.peek(4))
        
    def test_drop_part(self):
        self.target.enqueue("123")
        self.target.drop(3)
        self.assertEqual("", self.target.all())
        
    def test_drop_partial(self):
        self.target.enqueue("123")
        self.target.drop(2)
        self.assertEqual("3", self.target.all())
        
    def test_drop_two_parts(self):
        self.target.enqueue("123")
        self.target.enqueue("456")
        self.target.drop(3)
        self.target.drop(3)
        self.assertEqual("", self.target.all())
        
    def test_drop_overlap(self):
        self.target.enqueue("123")
        self.target.enqueue("456")
        self.target.drop(4)
        self.target.drop(2)
        self.assertEqual("", self.target.all())