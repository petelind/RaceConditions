from datetime import datetime
from random import randint
from threading import Thread
from time import sleep

from hash_table.hashtable import UnsafeHashTable, SafeHashTable, FastSafeHashTable
from linked_list.LinkedList import LinkedList
from node.Node import DecoratedNode

batch_size = 3000


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
    print("[ LL TIME ] Linked list creation finished in " + str(timer_end - timer_start))
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
    This one empties the list as it processes it.
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
    timer_start = datetime.now()
    summation(list_to_process, 'one-and-only', 1, batch_size)
    timer_end = datetime.now()
    print("[ LL TIME ] Linked dumb list processing finished in " + str(timer_end - timer_start))


def smart_processing(list_to_process):
    """
    "Smart" processor - we hand the list to the 4 workers to process, expecting ~4X better time...
    :param list_to_process:
    :return:
    """
    timer_start = datetime.now()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=summation, name='1st_linkedlist_processor',
                     args=(list_to_process, '1st_worker', 1, borderline))
    stream2 = Thread(target=summation, name='2nd__linkedlist_processor',
                     args=(list_to_process, '2nd_worker', borderline + 1, borderline * 2))
    stream3 = Thread(target=summation, name='3rd__linkedlist_processor',
                     args=(list_to_process, '3rd_worker', borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream1.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ LL TIME ] Linked list smart processing finished in " + str(timer_end - timer_start))

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
    print("[ UHT TIME ] UnsafeHashTable created in: " + str(timer_end - timer_start))
    print("[ UHT ] UnsafeHashTable has " + str(temp_table.NodeCount) + " elements")
    return temp_table


def safe_threaded_element_insert(hash_table, start_value, end_value):
    """
    This func not just inserts element, but waits its turn for insertion via polling the Lock of the HashTable
    :param hash_table:
    :param start_value:
    :param end_value:
    :return:
    """
    # TODO: Whats wrong about this lock? How can we improve?
    hash_table.lock.acquire()
    for i in range(start_value, end_value + 1):
        hash_table.add(i, 1)
    hash_table.lock.release()


def create_hashtable_safely():
    """
    Creates Hashtable filled with the same data as LinkedList before
    :return: HashTable with the data of the batch_size
    """
    timer_start = datetime.now()
    temp_table = SafeHashTable()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=safe_threaded_element_insert, name='1st_SHT_creator',
                     args=(temp_table, 1, borderline))
    stream2 = Thread(target=safe_threaded_element_insert, name='2nd_SHT_creator',
                     args=(temp_table, borderline + 1, borderline * 2))
    stream3 = Thread(target=safe_threaded_element_insert, name='3rd_SHT_creator',
                     args=(temp_table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ SHT TIME ] SafeHashTable created in: " + str(timer_end - timer_start))
    print("[ SHT ] SafeHashTable has " + str(temp_table.NodeCount) + " elements")
    return temp_table


def safe_fast_element_insert(temp_table: FastSafeHashTable, bucket_number: int):
    """
    This insert inserts only values of the special kind, thus locking only one LinkedList in HashTable
    :return: SafeHashTable size of the global batch_size
    """
    if bucket_number == 1:
        for i in range(1, batch_size + 1, 3):
            # print("[ INFO-1 ] Processing node #" + str(i))
            temp_table.add(i, 1)
            # print("[ INFO-1 ] Just processed node #" + str(i))
    if bucket_number == 2:
        for i in range(2, batch_size + 1, 3):
            # print("[ INFO-2 ] Processing node #" + str(i))
            temp_table.add(i, 1)
            # print("[ INFO-2 ] Just processed node #" + str(i))
    if bucket_number == 3:
        for i in range(3, batch_size + 1, 3):
            # print("[ INFO-3 ] Processing node #" + str(i))
            temp_table.add(i, 1)
            # print("[ INFO-3 ] Just processed node #" + str(i))
    if bucket_number not in [1, 2, 3]:
        raise ValueError(" [ ERROR ] Bucket number is out of range: " + str(bucket_number))


def fast_create_hashtable_safely():
    """
    Creates Hashtable filled with the same data as LinkedList before
    :return: HashTable with the data of the batch_size
    """
    timer_start = datetime.now()
    temp_table = FastSafeHashTable()

    stream1 = Thread(target=safe_fast_element_insert, name='1st_SFHT_creator', args=(temp_table, 1))
    stream2 = Thread(target=safe_fast_element_insert, name='2nd_SFHT_creator',
                     args=(temp_table, 2))
    stream3 = Thread(target=safe_fast_element_insert, name='3rd_SFHT_creator',
                     args=(temp_table, 3))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ FSHT TIME ] SafeHashTable created in: " + str(timer_end - timer_start))

    return temp_table


def read_nodes(table, start_value, end_value):
    for i in range(start_value, end_value + 1):
        # TODO: why this part is guarded with the Lock? what if it will be no lock here?
        if type(table) is SafeHashTable:
            with table.lock:
                node = table.get(i)
                table.Sum += node.value
        else:
            node = table.get(i)
            table.Sum += node.value


def process_hashtable(table):
    timer_start = datetime.now()
    borderline = int(batch_size / 3)
    stream1 = Thread(target=read_nodes, name='1st_HT_processor', args=(table, 1, borderline))
    stream2 = Thread(target=read_nodes, name='2nd_HT_processor',
                     args=(table, borderline + 1, borderline * 2))
    stream3 = Thread(target=read_nodes, name='3rd_HT_processor',
                     args=(table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    if type(table) is SafeHashTable:
        print("[ SHT ] SafeHashTable processed in: " + str(timer_end - timer_start))
    else:
        print("[ UHT ] UnsafeHashTable processed in: " + str(timer_end - timer_start))


def _fast_read_nodes(table: FastSafeHashTable, start_value: int, end_value: int):
    """
    This is a fast summator for the SFHT type
    :param end_value:
    :param start_value:
    :param table:
    :return: FSHT with the Sum updated
    """
    summ = 0
    for i in range(start_value, end_value + 1):
        next_node = table.get(i)
        summ += next_node.value
        # print(" [ INFO ] Running sum: " + str(summ))
    with table.lock:
        table.Sum += summ
        # print(" [ INFO ] Sum after thread : " + str(table.Sum))
        # TODO: Can I write to the table.Sum instead of my onw summ? What will happen?


def fast_read_nodes(table: FastSafeHashTable, bucket_number: int):
    summ = 0
    if bucket_number == 1:
        for i in range(1, batch_size + 1, 3):
            # print("[ INFO-1 ] Processing node #" + str(i))
            next_node = table.get(i)
            summ += next_node.value
            # print("[ INFO-1 ] Just processed node #" + str(i))
    if bucket_number == 2:
        for i in range(2, batch_size + 1, 3):
            # print("[ INFO-2 ] Processing node #" + str(i))
            next_node = table.get(i)
            summ += next_node.value
            # print("[ INFO-2 ] Just processed node #" + str(i))
    if bucket_number == 3:
        for i in range(3, batch_size + 1, 3):
            # print("[ INFO-3 ] Processing node #" + str(i))
            next_node = table.get(i)
            summ += next_node.value
            # print("[ INFO-3 ] Just processed node #" + str(i))
    if bucket_number not in [1, 2, 3]:
        raise ValueError(" [ ERROR ] Bucket number is out of range: " + str(bucket_number))
    with table.lock:
        table.Sum += summ


def _fast_safe_process_hashtable(table):
    timer_start = datetime.now()

    # We will create 3 separate worlkloads here, and submit them for processing...
    borderline = int(batch_size / 3)
    stream1 = Thread(target=fast_read_nodes, name='1st_FSHT_processor', args=(table, 1, borderline))
    stream2 = Thread(target=fast_read_nodes, name='2nd_FSHT_processor',
                     args=(table, borderline + 1, borderline * 2))
    stream3 = Thread(target=fast_read_nodes, name='3rd_FSHT_processor',
                     args=(table, borderline * 2 + 1, batch_size))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ FSHT ] FastSafeHashTable processed in: " + str(timer_end - timer_start))


def fast_safe_process_hashtable(table):
    timer_start = datetime.now()

    # We will create 3 separate worlkloads here, and submit them for processing...
    borderline = int(batch_size / 3)
    stream1 = Thread(target=fast_read_nodes, name='1st_FSHT_processor', args=(table, 1))
    stream2 = Thread(target=fast_read_nodes, name='2nd_FSHT_processor',
                     args=(table, 2))
    stream3 = Thread(target=fast_read_nodes, name='3rd_FSHT_processor',
                     args=(table, 3))

    stream1.start()
    stream2.start()
    stream3.start()

    stream1.join()
    stream2.join()
    stream3.join()

    timer_end = datetime.now()
    print("[ FSHT ] FastSafeHashTable processed in: " + str(timer_end - timer_start))


# TODO: Run both processing implementations, see whats the difference? Which one is faster/slower? Why?
# big_list = obtain_list()
# TODO: which benefits LinkedList (this implementation) has? Whats the drawbacks?
# dumb_processing(big_list)
# smart_processing(big_list)
# TODO: Why does parallel processing do not work?

# Ok, now lets have a look on the HashTable...
# It should be fast because we employ multiple threads to build the table, right?
corrupt_table = create_hashtable()
process_hashtable(corrupt_table)
print("[ UHT ] UnsafeHashTable Sum of Node values: " + str(corrupt_table.Sum))
# TODO: What happened here? Why the number of elements is not batch_size?

safe_table = create_hashtable_safely()
process_hashtable(safe_table)
print("[ SHT ] SafeHashTable Sum of Node values: " + str(safe_table.Sum))
# TODO: What happens here? Why the sum is correct? Why processing is so slow?

fast_table = fast_create_hashtable_safely()
print("[ FSHT ] FastSafeHashTable has " + str(fast_table.NodeCount) + " elements")
fast_safe_process_hashtable(fast_table)
print("[ FSHT ] FastSafeHashTable Sum of Node values: " + str(fast_table.Sum))
# TODO: What happens here? Why the sum is correct? How come processing is faster?
