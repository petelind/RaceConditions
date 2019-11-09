from threading import Lock

from node.Node import DecoratedNode


class LinkedList:
    def __init__(self):
        self.Head = None
        self.Tail = None
        self.NodeCount = 0
        self.Sum = 0  # To be updated later on with some special worker

    def add_node(self, node: DecoratedNode):
        if self.Head is None:
            self.Head = node
            self.Tail = node
            self.NodeCount = 1
            return

        self.Tail.Next = node
        self.Tail = node
        self.NodeCount += 1

        return

    def __str__(self):
        next_node = self.Head
        descriptions = ''
        while next_node is not None:
            descriptions += next_node.__str__() + ', '
            next_node = next_node.Next
        return descriptions[:-2]

    def find_node(self, key: int):
        """
        Returns the Node found by key
        :param key: key to look for as int
        :return: Node if found, None if there is no such element
        """
        next_node = self.Head
        while next_node is not None:
            if next_node.key == key:
                return next_node
            next_node = next_node.Next
        return None

    def get_head(self) -> DecoratedNode:
        if self.Head is not None:
            top = self.Head
            self.Head = self.Head.Next
            self.NodeCount -= 1
            return top
        return None


class SafeLinkedList(LinkedList):
    def __init__(self):
        super().__init__()
        self.lock = Lock()

# TODO: Implement Queue (FIFO)
# TODO: Implement Stack (LIFO)
