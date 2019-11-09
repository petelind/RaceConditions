from unittest import TestCase

from linked_list.LinkedList import LinkedList
from node.Node import DecoratedNode


class TestLinkedList(TestCase):
    def test_PutElements_ExpectCorrectCount(self):

        # arrange
        batch_size = 10
        linked_list = LinkedList()

        # act
        for i in range(1, batch_size+1):
            new_node = DecoratedNode(i, i*10)
            linked_list.add_node(new_node)

        print(linked_list)

        # assert
        self.assertEqual(batch_size, linked_list.NodeCount)

    def test_PutElements_ExpectThemToBePresent(self):
        # arrange
        batch_size = 10
        linked_list = LinkedList()
        original_descriptions = []

        # act
        for i in range(1, batch_size + 1):
            new_node = DecoratedNode(i, i * 10)
            original_descriptions.append(new_node.__str__())
            linked_list.add_node(new_node)

        resulting_descriptions = linked_list.__str__().split(', ')

        # assert
        for resulting_description in resulting_descriptions:
            self.assertIn(resulting_description, original_descriptions)