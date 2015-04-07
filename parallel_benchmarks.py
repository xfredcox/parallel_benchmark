import time
import multiprocessing as mp
import random
import os
import threading
import Queue
import logging
import unittest

log = logging
log.basicConfig(level="DEBUG")

MAX_PROCESSES = 8 
MAX_THREADS = 8
TIMEOUT = 2

def IO_BOUND(*args, **kwargs):
    log.debug("IO :: {} :: {}".format(os.getpid(), threading.currentThread().name))
    time.sleep(random.random() * TIMEOUT)
    global OUTPUT    
    OUTPUT.put(["IO_BOUND", os.getpid(), threading.currentThread().name])    

def CPU_BOUND(*args, **kwargs):
    log.debug("CPU :: {} :: {}".format(os.getpid(), threading.currentThread().name))
    _ = [x**x for x in range(5500)]
    # Takes about 1s for me
    global OUTPUT
    OUTPUT.put(["CPU_BOUND", os.getpid(), threading.currentThread().name])

RATIO = 4 # Ratio of IOs per CPU calls (must be int)
CPUs = 1

_IOs = [IO_BOUND for _ in range(RATIO * CPUs)]
TASKS = random.sample([CPU_BOUND for _ in range(CPUs)] + _IOs, CPUs + RATIO * CPUs)
OUTPUT = Queue.Queue() # Empty container to run tests that the functions actually got called.

def executer(name):
    if name == 'CPU_BOUND':
        return CPU_BOUND()
    elif name == 'IO_BOUND':
        return IO_BOUND()

def threaded_executer(*args, **kwargs):

    def target_func():
        log.debug("CALLED")
        try:
            task = q.get_nowait()
        except:
            log.debug("EXCEPT")
            return
        while task:
            executer(task)
            try:
                task = q.get_nowait()
            except Queue.Empty:
                task = None
    
    threads = []

    for i in range(MAX_THREADS):
        log.debug("THREAD {}".format(i))
        thread = threading.Thread(target=target_func, name=str(i))
        threads.append(thread)
        thread.setDaemon(True)
        thread.start()

q = Queue.Queue()
for t in [x.__name__ for x in TASKS]:
    q.put(t)        

def model_1():
    # Sequential Benchmark
    ## Single Process w/ Synchronous Calls

    log.debug("===== Running Model I =====")
    start_time = time.time()
    n = len([x() for x in TASKS])
    total_time = time.time() - start_time

    txt = "===== model_1 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)
    
    return txt, total_time / n

def model_2():
    # Multiprocessed Benchmark
    ## N Processes w/ Synchronous Calls

    log.debug("===== Running Model II =====")
    start_time = time.time()
    pool = mp.Pool(MAX_PROCESSES)
    n = len(pool.map(executer, [x.__name__ for x in TASKS]))
    total_time = time.time() - start_time

    txt = "===== model_2 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)

    return txt, total_time / n    

def model_3():
    # Asyncronous Benchmark
    ## Single Process w/ Async Calls

    log.debug("===== Running Model III =====")
    start_time = time.time()
    
    q = Queue.Queue()
    for t in TASKS:
        q.put(t)

    def target_func():
        try:
            task = q.get_nowait()
        except:
            return 
        while task:
            task()
            try:
                task = q.get_nowait()
            except Queue.Empty:
                task = None

    threads = []

    for i in range(MAX_THREADS):
        thread = threading.Thread(target=target_func, name=str(i))
        threads.append(thread)
        thread.setDaemon(True)
        thread.start()

    n = len(TASKS)

    [t.join() for t in threads]
    
    total_time = time.time() - start_time

    txt = "===== model_3 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)

    return txt, total_time / n    
    
def model_4():
    # Multiprocessed Asyncronous
    ## N Processes w/ Async Calls
    log.debug("===== Running Model IV =====")
    start_time = time.time()
    
    pool = mp.Pool(MAX_PROCESSES)

    n = len(pool.map(threaded_executer, [x.__name__ for x in TASKS]))

    total_time = time.time() - start_time

    txt = "===== model_4 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)

    return txt, total_time / n    
    
def main():    
    txt1, t1 =  model_1()
    txt2, t2 =  model_2()
    txt3, t3 =  model_3()
    txt4, t4 =  model_4()    

    log.info(txt1)
    log.info(txt2)
    log.info(txt3)
    log.info(txt4) 

    log.info("\n1 : {} : {} : {}".format(round(t2/t1, 4), round(t3/t1, 4), round(t4/t1, 4)))
    
    return

def test_model(model):
    global OUTPUT
    clearQueue(OUTPUT)
    
    logging.info(model()[0])

    print OUTPUT
    
    list_output = deQueue(OUTPUT)

    print list_output
    
    assert len(list_output) == len(TASKS)
    funcs = [x[0] for x in list_output]
    pids = [x[1] for x in list_output]
    threads = [x[2] for x in list_output]    

    assert sorted(funcs) == sorted([x.__name__ for x in TASKS])
    assert len(set(pids)) <= MAX_PROCESSES
    assert len(set(threads)) <= MAX_THREADS

    logging.info("PASSED TESTS")

####### START OF QUEUE METHODS
    
def test_deQueue():
    q = Queue.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    l = deQueue(q)

    assert l == [1,2,3]

    logging.info("test_deQueue Passed")

def test_clearQueue():
    q = Queue.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    # By assignment
    q = clearQueue(q)
    assert q.qsize() == 0

    q = Queue.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    # In place
    clearQueue(q)
    assert q.qsize() == 0    

    logging.info("test_clearQueue Passed")    
    
def deQueue(q):
    output = []
    while True:
        try:
            el = q.get_nowait()
            output.append(el)
        except Queue.Empty:
            return output

def clearQueue(q):
    while True:
        try:
            el = q.get_nowait()
        except Queue.Empty:
            return q

####### END OF QUEUE METHODS

####### START OF FILE METHODS

FILE_NAME = 'test_exec.txt'

def clear_file():
    with open(FILE_NAME, 'w') as f:
        f.write('')

def write_line(txt):
    with open(FILE_NAME, 'r+') as f:
        f.write(txt)

def deFile():
    with open(FILE_NAME, 'r') as f:
        return f.read().split("\n")

def test_clear_file():
    f = open(FILE_NAME, 'w')
    f.write('TEST')
    f.close()

    clear_file()

    f = open(FILE_NAME, 'r')
    txt = f.read()

    assert txt == ''

    logging.info("test_clear_file Passed")        

def test_write_line():
    clear_file()

    write_line('WRITE TEST')
    
    f = open(FILE_NAME, 'r')
    txt = f.read()

    assert txt == 'WRITE TEST'

    clear_file()

    write_line('TEST')
    write_line('WRITE\n')
    
    f = open(FILE_NAME, 'r')
    txt = f.read()

    print txt
    assert txt == 'WRITE\nTEST'    

    logging.info("test_write_line Passed")

def test_deFile():
    clear_file()

    write_line('3\n')
    write_line('2\n')
    write_line('1\n')        
    
    l = deFile()

    print l
    assert l == [1,2,3]

    logging.info("test_deFile Passed")            
    
####### END OF FILE METHODS

if __name__ == "__main__":
    #main()
    test_model(model_2)


