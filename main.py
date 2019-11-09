from datetime import datetime
from random import randint
from threading import Thread
from time import sleep

from hash_table.hashtable import UnsafeHashTable, SafeHashTable
from linked_list.LinkedList import LinkedList
from node.Node import DecoratedNode

batch_size = 48000


def obtain_list():
    """
    Creates linked list of a given batch size.
    :return: Returns a linked list which contains of key 1 to batch_size and values which are 1
    """
    timer_start = datetime.now()

    linked_list = LinkedList()
    for i in range(1, batch_size + 1):
        new_node = DecoratedNode(i, 1)
        linked_list.add_node(new_node)

    timer_end = datetime.now()
    print("[ LINKEDLIST ] Linked list creation finished in " + str(timer_end - timer_start))
    return linked_list


def summation(linked_list, name, start_value, end_value):
    """
    Emulates some kind of scientific computation - like a sigma summation running on the set of data
    :param linked_list:
    :param name:
    :return: records Sum into the LinkedList
    """
    for i in range(start_value, end_value + 1):
        # print(" [ LINKEDLIST ] Processing Node " + str(i))
        next_node = linked_list.find_node(i)
        linked_list.Sum += next_node.value


def destructive_summation(linked_list, name, stub1, stub2):
    """
    Emulates some kind of scientific computation - like a sigma summation running on the set of data
    :param linked_list:
    :param name:
    :return:
    """
    processed_nodes = 0
    next_node = linked_list.get_head()
    while next_node is not None:
        # print("[ LINKEDLIST ] Thread: " + name + " processing Node:  " + str(next_node.key))
        # emulates computations...
        linked_list.Sum += next_node.value
        next_node = linked_list.get_head()
        processed_nodes += 1
        # print(" [ LINKEDLIST ] Thread " + name + " processed " + str(processed_nodes))
    return linked_list.Sum


def dumb_processing(list_to_process):
    """
    Dumb processor - just works through elements one by one, takes forever.
    :param list_to_process:
    :return: will print out t
    """
    summation(list_to_process, 'one-and-only', 1, batch_size)


def smart_processing(list_to_process):
    """
    "Smart" processor - we hand the list to the 4 workers to process, expecting ~4X better time...
    :param list_to_process:
    :return:
    """
    borderline = int(batch_size / 3)
    stream1 = Thread(target=destructive_summation, name='1st_linkedlist_processor',
                     args=(list_to_process, '1st_worker', 1, borderline))
    stream2 = Thread(target=destructive_summation, name='2nd__linkedlist_processor',
                     args=(list_to_process, '2nd_worker', borderline + 1, borderline * 2))
    stream3 = Thread(target=destructive_summation, name='3rd__linkedlist_processor',
                     args=(list_to_process, '3rd_worker', borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    print("[ LINKEDLIST ] all workers launched...")

    stream1.join()
    stream1.join()
    stream3.join()
    print("[ LINKEDLIST ] all workers finished processing...")

    return


def threaded_element_insert(hash_table, start_value, end_value):
    """
    This func is capable to insert elements while running inside the thread.
    :param hash_table:
    :param start_value:
    :param end_value:
    :return:
    """
    for i in range(start_value, end_value + 1):
        hash_table.add(i, 1)


def safe_threaded_element_insert(hash_table, start_value, end_value):
    """
    This func not just inserts element, but waits its turn for insertion via polling the Lock of the HashTable
    :param hash_table:
    :param start_value:
    :param end_value:
    :return:
    """

    for i in range(start_value, end_value + 1):
        with hash_table.lock:
            hash_table.add(i, 1)


def create_hashtable():
    """
    Creates Hashtable filled with the same data as linked list before
    :return:
    """
    timer_start = datetime.now()
    temp_table = UnsafeHashTable()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=threaded_element_insert, name='1st_hashtable_creator', args=(temp_table, 1, borderline))
    stream2 = Thread(target=threaded_element_insert, name='2nd__hashtable_creator',
                     args=(temp_table, borderline + 1, borderline * 2))
    stream3 = Thread(target=threaded_element_insert, name='3rd__hashtable_creator',
                     args=(temp_table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ UHT ] UnsafeHashTable created in: " + str(timer_end - timer_start))

    return temp_table


def create_hashtable_safely():
    """
    Creates Hashtable filled with the same data as LinkedList before
    :return: HashTable with the data of the batch_size
    """
    timer_start = datetime.now()
    temp_table = SafeHashTable()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=safe_threaded_element_insert, name='1st__hashtable_creator', args=(temp_table, 1, borderline))
    stream2 = Thread(target=safe_threaded_element_insert, name='2nd__hashtable_creator',
                     args=(temp_table, borderline + 1, borderline * 2))
    stream3 = Thread(target=safe_threaded_element_insert, name='3rd__hashtable_creator',
                     args=(temp_table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ SHT ] SafeHashTable created in: " + str(timer_end - timer_start))

    return temp_table


def read_nodes(table, start_value, end_value):
    for i in range(start_value, end_value + 1):
        node = table.get(i)
        # TODO: why this part is guarded with the Lock? what if it will be no lock here?
        if type(table) is SafeHashTable:
            with table.lock:
                table.Sum += node.value
        else:
            table.Sum += node.value

def process_hashtable(table):
    timer_start = datetime.now()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=read_nodes, name='1st_hashtable_processor', args=(table, 1, borderline))
    stream2 = Thread(target=read_nodes, name='2nd_hashtable_processor',
                     args=(table, borderline + 1, borderline * 2))
    stream3 = Thread(target=read_nodes, name='3rd_hashtable_processor',
                     args=(table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ SHT ] SafeHashTable created in: " + str(timer_end - timer_start))


timer_start = datetime.now()

# TODO: Run both processing implementations, see whats the difference? Which one is faster/slower?
big_list = obtain_list()
# dumb_processing(big_list)
smart_processing(big_list)

# TODO: which benefits LinkedList (this implementation) has? Whats the drawbacks?

timer_end = datetime.now()
print("[ LINKEDLIST ] Sum: " + str(big_list.Sum) + ", processed in: " + str(
    timer_end - timer_start) + ", elements in list: " + str(big_list.NodeCount))

# Ok, now lets have a look on the HashTable...
corrupt_table = create_hashtable()
print("[ UHT ] UnsafeHashTable has " + str(corrupt_table.NodeCount) + " elements")
process_hashtable(corrupt_table)
print("[ UHT ] UnsafeHashTable Sum of Node values: " + str(corrupt_table.Sum))
# TODO: What happened here? Why the number of elements is not batch_size?

safe_table = create_hashtable_safely()
print("[ SHT ] SafeHashTable has " + str(safe_table.NodeCount) + " elements")
process_hashtable(safe_table)
print("[ SHT ] SafeHashTable Sum of Node values: " + str(safe_table.Sum))
