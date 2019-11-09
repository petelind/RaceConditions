class Node:
    def __init__(self, key: int, value: int):
        self.key = key
        self.value = value
        self.Next = None
        self.isHead = None
        # all these are accessible!!! No encapsulation exist! (This is why c* people really hate it :))

    def isHead(self):
        """
        This is a doc string. The purpose of the method is to tell if its the head of linked list
        :return: Bool with the result of the check, none if node is not associated.
        """
        if self.isHead:
            return True
        return False

    def __str__(self):
        return "Node: " + str(self.key) + " Value: " + str(self.value)


class DecoratedNode(Node):
    def __init__(self, key: int, value: int):
        super().__init__(key, value)

    @property
    def is_head(self) -> bool:
        if self.isHead is False or self.isHead is None:
            return False
        return True

    @is_head.setter
    def is_head(self, value=True):
        self.isHead = value
        # self.isHead = true what will happen? this is why so many people hate dynamic typing :)





