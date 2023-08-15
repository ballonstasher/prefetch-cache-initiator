from extent import *

class Item():
    def __init__(self, k, v = None):
        self.k = k
        self.v = v

    def __gt__(self, other):
        if self.k > other.k:
            return True
        else:
            return False

    def __ge__(self, other):
        if self.k >= other.k:
            return True
        else:
            return False

    def __eq__(self, other):
        if self.k == other.k:
            return True
        else:
            return False

    def __le__(self, other):
        if self.k <= other.k:
            return True
        else:
            return False

    def __lt__(self, other):
        if self.k < other.k:
            return True
        else:
            return False

class Node:
    def __init__(self):
        self.children = []
        self.items = []

    def __repr__(self):
        return 'Node' + str(self.items) + str(self.children)

    def _lower_bound(self, item):
        b = 0
        e = len(self.children) - 1
        while b < e:
            mid = (b + e + 1) // 2
            if mid == 0:
                pass
            elif self.items[mid - 1] <= item:
                b = mid
            else:
                e = mid - 1
        return b

class ExtentTree:
    def __init__(self, t):
        self.root = Node()
        self.t = t
    
    def _inorder(self, cur):
        if cur == None: return
        
        for i, child in enumerate(cur.children):
            if i > 0:
                yield cur.items[i - 1]
            yield from self._inorder(child)
    
    def inorder(self):
        yield from self._inorder(self.root)
    
    def _preorder(self, cur):
        if cur == None: return
        for item in cur.items:
            yield item
        for child in cur.children:
            yield from self._preorder(child)
    
    
    def preorder(self):
        yield from self._preorder(self.root)
        
    def _split(self, node, parnode, pos):
        # root case
        if parnode is None:
            self.root = Node()
            left = Node()
            right = Node()
            left.items = node.items[:self.t - 1]
            right.items = node.items[self.t:]
            left.children = node.children[:self.t]
            right.children = node.children[self.t:]
            self.root.items = [ node.items[self.t - 1] ]
            self.root.children = [left, right]
            return self.root
        else:
            left = Node()
            right = Node()
            left.items = node.items[:self.t - 1]
            right.items = node.items[self.t:]
            left.children = node.children[:self.t]
            right.children = node.children[self.t:]
            parnode.items = parnode.items[:pos] + [ node.items[self.t - 1] ] + parnode.items[pos:]
            parnode.children = parnode.children[:pos] + [left, right] + parnode.children[pos + 1:]
            
    def _insert(self, item, node, parnode):
        if node is None: return None

        # node is full, and must be root
        if len(node.items) == 2 * self.t - 1:
            assert node == self.root
            node = self._split(node, parnode, -1)
            assert len(node.items) == 1
            
            # to the right
            if node.items[0] <= item:
                self._insert(item, node.children[1], node)
            else:
                self._insert(item, node.children[0], node)
            
            return
        
        # only possible for root at the beginning
        if len(node.children) == 0:
            assert node == self.root
            node.children.append(None)
            node.items.append(item)
            node.children.append(None)
        
            return
        
        pos = node._lower_bound(item)

        # we are in a leaf
        if node.children[pos] is None:
            node.items = node.items[:pos] + [item] + node.items[pos:]
            node.children.append(None)
        else:
            # child is full, doing split from here
            if node.children[pos] is not None and len(node.children[pos].items) == 2 * self.t - 1:
                self._split(node.children[pos], node, pos)
                # go to right
                if node.items[pos] <= item:
                    self._insert(item, node.children[pos + 1], node)
                else:
                    self._insert(item, node.children[pos], node)
            else:
                self._insert(item, node.children[pos], node)

    
    
    def insert(self, item):
        self._insert(item, self.root, None)
    
    def _find(self, item, node):
        if node is None or len(node.children) == 0:
            return None
        
        pos = node._lower_bound(item)
        
        if pos >= 1 and (node.items[pos - 1] == item or 
                extent_hold_key(node.items[pos - 1].v, item.k)):
            return node.items[pos - 1]
        else:
            return self._find(item, node.children[pos])
         
         
    def find(self, item):
        return self._find(item, self.root)
    
    def _find_predecessor(self, item, node):
        if node.children[0] == None:
            return node.items[-1]
        else:
            return self._find_predecessor(item, node.children[-1])
    
    def _find_succesor(self, item, node):
        if node.children[0] == None:
            return node.items[0]
        else:
            return self._find_succesor(item, node.children[0])
    
    def _delete_item_leaf(self, item, node, pos):
        # condition for correctness of algorithm
        assert node == self.root or len(node.children) >= self.t
        assert node.items[pos] == item
        
        node.items = node.items[:pos] + node.items[pos + 1:]
        node.children.pop()
        
        
    def _merge_children_around_item(self, item, node, pos):
        assert pos >= 0 and pos < len(node.children) - 1
        y = Node()
        y.children = node.children[pos].children + node.children[pos + 1].children
        y.items = node.children[pos].items + [node.items[pos]] + node.children[pos + 1].items
        
        node.items = node.items[:pos] + node.items[pos + 1:]
        node.children = node.children[:pos] + [y] + node.children[pos + 2:]
        
    def _move_node_from_left_child(self, node, pos):
        assert pos > 0 and len(node.children[pos - 1].items) >= self.t
        
        node.children[pos].items = [node.items[pos - 1] ] + node.children[pos].items
        node.children[pos].children = [ node.children[pos - 1].children[-1] ] + node.children[pos].children
        
        node.items[pos - 1] = node.children[pos - 1].items[-1]
        
        node.children[pos - 1].children = node.children[pos - 1].children[:-1]
        node.children[pos - 1].items = node.children[pos - 1].items[:-1]
        
    def _move_node_from_right_child(self, node, pos):
        assert pos < len(node.children) - 1 and len(node.children[pos + 1].items) >= self.t
        
        node.children[pos].items = node.children[pos].items + [node.items[pos] ]
        node.children[pos].children =  node.children[pos].children + [ node.children[pos + 1].children[0] ] 
        
        node.items[pos] = node.children[pos + 1].items[0]
        
        node.children[pos + 1].children = node.children[pos + 1].children[1:]
        node.children[pos + 1].items = node.children[pos + 1].items[1:]
        
    def _fix_empty_root(self, node):
        if node == self.root and len(node.children) == 1:
            self.root = node.children[0]
            return self.root
        else:
            return node
    
    
    def _delete(self, item, node):
        if node is None or len(node.children) == 0: return
        
        pos = node._lower_bound(item)
        
        # the item to delete is here
        if pos > 0 and node.items[pos - 1] == item:
            
            # this node is a leaf
            if node.children[pos] is None:
                self._delete_item_leaf(item, node, pos - 1)
            # left child node has enough items
            elif len(node.children[pos - 1].items) >= self.t:
                kp = self._find_predecessor(item, node.children[pos - 1])
                node.items[pos - 1] = kp
                self._delete(kp, node.children[pos - 1])
            # right child node has enough items
            elif len(node.children[pos].items) >= self.t:
                kp = self._find_succesor(item, node.children[pos])
                node.items[pos - 1] = kp
                self._delete(kp, node.children[pos])
            # both children have minimal number of items, must combine them
            else:
                self._merge_children_around_item(item, node, pos - 1)
                
                # here I should take care of missing root
                node = self._fix_empty_root(node)
                
                self._delete(item, node)
        else:
            
            # we are on a leave and haven't found the item, we have nothing to do
            if node.children[pos] is None:
                pass
            # the amount of items in the child is enough, simply recurse
            elif len(node.children[pos].items) >= self.t:
                self._delete(item, node.children[pos])
            # we must push a item to the child
            else:
                # left sibbling has enough items
                if pos > 0 and len(node.children[pos - 1].items) >= self.t:
                    self._move_node_from_left_child(node, pos)
                    self._delete(item, node.children[pos])
                # right sibbling has enough items
                elif pos < len(node.children) - 1 and len(node.children[pos + 1].items) >= self.t:
                    self._move_node_from_right_child(node, pos)
                    self._delete(item, node.children[pos])
                # must merge with one of sibblings
                else:
                    
                    if pos > 0:
                        self._merge_children_around_item(item, node, pos - 1)
                        
                        # here I should take care of missing root
                        node = self._fix_empty_root(node)
                        
                        self._delete(item, node)
                    elif pos < len(node.children) - 1:
                        self._merge_children_around_item(item, node, pos)
                        
                        # here I should take care of missing root
                        node = self._fix_empty_root(node)
                        
                        self._delete(item, node)
                    # this shouldn't be possible
                    else:
                        assert False
        
    def delete(self, item):
        self._delete(item, self.root)
        
    def _find_all(self, item, node, ans):
        if node is None or len(node.children) == 0: return
        b = 0
        e = len(node.children) - 1
        while b < e:
            mid = (b + e + 1) // 2
            if mid == 0: # mid is never 0 actually
                pass
            elif node.items[mid - 1] < item:
                b = mid
            else:
                e = mid - 1
        
        left = b
        
        b = 0
        e = len(node.children) - 1
        while b < e:
            mid = (b + e + 1) // 2
            if mid == 0: # mid is never 0 actually
                pass
            elif node.items[mid - 1] > item:
                e = mid - 1
            else:
                b = mid
        right = b
        
        # print(left, right, len(node.children))
        for i in range(left, right + 1):
            self._find_all(item, node.children[i], ans)
            
            if i < right:
                assert node.items[i] == item
                ans.append(node.items[i])
        
    def find_all(self, item):
        ans = []
        self._find_all(item, self.root, ans)
        return ans 
            

def walk_test():
    B = ExtentTree(3)
    
    file_size = 1 << 20
    extents = gen_extent_list(file_size)

    for ext in extents:
        B.insert(Item(ext.lba, ext))

    #print(B.root)
    #print(list(B.preorder()))
    #print(list(B.inorder()))
    
    for ext in extents:
        lba = random.randint(ext.lba, ext.lba + ext.length - 1)
        item = B.find(Item(lba))
        if item:
            print(f"found={lba} {item.k} {item.v.lba + item.v.length}")
        else:
            print(f"Not found={lba}")


def main(args):
    walk_test()
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
