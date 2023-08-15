from stats import *

class DLLNode:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None


class DoublyLinkedList:
    def __init__(self):
        self.head = DLLNode(None, None)
        self.tail = DLLNode(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head

    def add_node(self, node):
        node.next = self.head.next
        node.prev = self.head
        self.head.next.prev = node
        self.head.next = node

    def remove_node(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = None
        node.next = None

    def move_to_front(self, node):
        self.remove_node(node)
        self.add_node(node)

    def get_oldest_node(self):
        if self.tail.prev != self.head:
            return self.tail.prev
        return None


class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.linked_list = DoublyLinkedList()

    def get(self, key):
        if key in self.cache:
            """ move to mru """
            node = self.cache[key]
            self.linked_list.move_to_front(node) 
            Stats.ext_cache_hit += 1
            return node.value
        else:
            Stats.ext_cache_miss += 1
            return None
    
    def evict(self):
        ev_key = None
        oldest_node = self.linked_list.get_oldest_node()
        if oldest_node:
            ev_key = oldest_node.key
            del self.cache[oldest_node.key]
            self.linked_list.remove_node(oldest_node)
            Stats.ext_cache_evict += 1
        return ev_key

    def put(self, key, value):
        ev_key = None
        if key in self.cache:
            """ existing entry, update value """
            node = self.cache[key]
            node.value = value
            self.linked_list.move_to_front(node)
            Stats.ext_cache_hit += 1
        else:
            """ first admission """
            if len(self.cache) >= self.capacity:
                ev_key = self.evict()

            new_node = DLLNode(key, value)
            self.cache[key] = new_node
            self.linked_list.add_node(new_node)
            Stats.ext_cache_miss += 1

        # TODO: remove me
        Stats.ext_cache_size = len(self.cache)

        return ev_key

    def delete(self, key):
        if key in self.cache:
            node = self.cache[key]
            self.linked_list.remove_node(node)
            del self.cache[node.key]
            Stats.ext_cache_inv += 1
            return node.value
        return None


if __name__ == "__main__":
    cache = LRUCache(2)

    cache.put(1, 'apple')
    cache.put(2, 'banana')

    print(cache.get(1))
    print(cache.get(2))

    cache.put(3, 'cherry')

    print(cache.get(1))
    print(cache.get(2))
    print(cache.delete(3))
    print(cache.get(3))
