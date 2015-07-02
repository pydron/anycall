'''
Created on 02.07.2015

@author: stefan
'''
import collections

class ByteQueue(object):
    """
    Efficient alternative to string concatenation.
    Bytes can be appended at the end and removed from the beginning
    of the string.    
    """
    
    def __init__(self):
        self._parts = collections.deque()
        self._len = 0
    
    def enqueue(self, s):
        """
        Append `s` to the queue.
        
        Equivalent to::
        
            queue += s
            
        if `queue` where a regular string.
        """
        self._parts.append(s)
        self._len += len(s)
        
    def dequeue(self, n):
        """
        Remove and return the first `n` characters from the queue.
        Throws an error if there are less than `n` characters in the queue.
        
        Equivalent to::
        
            s = queue[:n]
            queue = queue[n:]
            
        if `queue` where a regular string.
        """
        if self._len < n:
            raise ValueError("Not enough bytes in the queue")
        self._len -= n
        
        def part_generator(n):
            """
            Returns the requested bytes in parts
            """
            remaining = n
            while remaining:
                part = self._parts.popleft()
                if len(part) <= remaining:
                    yield part
                    remaining -= len(part)
                else:
                    yield part[:remaining]
                    self._parts.appendleft(part[remaining:])
                    remaining = 0
                    
        return "".join(part_generator(n))
    
    def drop(self, n):
        """
        Removes `n` bytes from the beginning of the queue.
        
        Throws an error if there are less than `n` characters in the queue.
        
        Equivalent to::
        
            queue = queue[n:]
            
        if `queue` where a regular string.
        """
        if self._len < n:
            raise ValueError("Not enough bytes in the queue")
        self._len -= n
        

        remaining = n
        while remaining:
            part = self._parts.popleft()
            if len(part) <= remaining:
                remaining -= len(part)
            else:
                self._parts.appendleft(part[remaining:])
                remaining = 0
                    
        
    
    def peek(self, n):
        """
        Return the first `n` characters from the queue without
        removing them.
        Throws an error if there are less than `n` characters in the queue.
        
        Equivalent to::
        
            s = queue[:n]
            
        if `queue` where a regular string.
        """
        if self._len < n:
            raise ValueError("Not enough bytes in the queue")
        
        def part_generator(n):
            """
            Returns the requested bytes in parts
            """
            
            remaining = n
            
            for part in self._parts:
                if len(part) <= remaining:
                    yield part
                    remaining -= len(part)
                else:
                    yield part[:remaining]
                    remaining = 0 
                if remaining == 0:
                    break
                    
        return "".join(part_generator(n))
    
    def all(self):
        """
        Returns all bytes currently in the queue without removing them.
        """
        return "".join(self._parts)
        
    def __len__(self):
        """
        Returns the number of characters in the queue.
        """
        return self._len