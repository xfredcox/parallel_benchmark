import time
import multiprocessing as mp
import random
import os
import threading
import logging
import unittest

log = logging
log.basicConfig(level="DEBUG")

MAX_PROCESSES = 1 
MAX_THREADS = 3
TIMEOUT = 2

def execute_by_name(name):
    if name == 'CPU_BOUND':
        return CPU_BOUND()
    elif name == 'IO_BOUND':
        return IO_BOUND()

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
CPUs = 2

_IOs = [IO_BOUND for _ in range(RATIO * CPUs)]
TASKS = random.sample([CPU_BOUND for _ in range(CPUs)] + _IOs, CPUs + RATIO * CPUs)
OUTPUT = mp.Queue() # Empty container to run tests that the functions actually got called.

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
    n = len(pool.map(execute_by_name, [x.__name__ for x in TASKS]))
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
    
    q = mp.Queue()
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
            except mp.queues.Empty:
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

####  START OF MODEL 4 ####

def threaded_executer(*args, **kwargs):

    log.debug("STARTING UP PROCESS {}".format(os.getpid()))
    
    def target_func():
        try:
            task = q.get_nowait()
            log.debug("Calling First Task :: {} :: {} :: {}".format(os.getpid(), threading.currentThread().name, task))
        except Exception as e:
            log.debug("Exit (UNEMPLOYED) Thread Called :: Process <{}> :: Thread <{}> :: Message {}".format(os.getpid(), threading.currentThread().name, type(e)))
            return
        while task:
            execute_by_name(task)
            try:
                task = q.get_nowait()
            except mp.queues.Empty as e:
                log.debug("Exit (RETIRED) Thread Called :: Process <{}> :: Thread <{}> :: Message {}".format(os.getpid(), threading.currentThread().name, type(e)))           
                task = None                
     
    threads = []

    for i in range(MAX_THREADS):
        thread = threading.Thread(target=target_func, name=str(i))
        threads.append(thread)
        thread.setDaemon(True)
        thread.start()

    [t.join() for t in threads]

    log.debug("SHUTTING DOWN PROCESS {}".format(os.getpid()))    
        
q = mp.Queue()
for t in [x.__name__ for x in TASKS]:
    q.put(t)        

def model_4():
    # Multiprocessed Asyncronous
    ## N Processes w/ Async Calls
    log.debug("===== Running Model IV =====")
    start_time = time.time()
    
    pool = mp.Pool(MAX_PROCESSES)

#    pool.map(threaded_executer, range(MAX_PROCESSES))
    pool.map(threaded_executer, range(MAX_PROCESSES))

    n = len(TASKS)
    
    total_time = time.time() - start_time

    txt = "===== model_4 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)

    return txt, total_time / n    


#### END OF MODEL 4 ####

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

    list_output = deQueue(OUTPUT)

    assert len(list_output) == len(TASKS)
    funcs = [x[0] for x in list_output]
    pids = [x[1] for x in list_output]
    threads = [x[2] for x in list_output]    

    assert sorted(funcs) == sorted([x.__name__ for x in TASKS])
    assert len(set(pids)) <= MAX_PROCESSES
    assert len(set(threads)) <= MAX_THREADS

    logging.info("PASSED TESTS\n")

####### START OF QUEUE METHODS
    
def test_deQueue():
    q = mp.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    l = deQueue(q)

    assert l == [1,2,3]

    logging.info("test_deQueue Passed")

def test_clearQueue():
    q = mp.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    # By assignment
    q = clearQueue(q)
    assert q.qsize() == 0

    q = mp.Queue()
    q.put(1)
    q.put(2)
    q.put(3)

    # In place
    clearQueue(q)
    assert q.qsize() == 0    

    logging.info("test_clearQueue Passed")    

    
def temp_func(i):
    test_q.put(i)

test_q = mp.Queue()
    
def test_queue():

    pool = mp.Pool(2)

    clearQueue(test_q)
    
    pool.map(temp_func, range(3))
    
    l = deQueue(test_q)

    assert set(l) == set([0,1,2])
    
def deQueue(q):
    output = []
    while True:
        try:
            el = q.get_nowait()
            output.append(el)
        except mp.queues.Empty:
            return output

def clearQueue(q):
    while True:
        try:
            el = q.get_nowait()
        except mp.queues.Empty:
            return q

####### END OF QUEUE METHODS

####### START OF ANALYSIS RUNNER

# GIVEN a set of production hardward (CORES, MEMORY, etc.)

# Range of CPU_BOUND function calls 1 - 10000

# Range of IO/CPU BOUND Ratios 1 - 100

pass

# Rander charts

# Estimate the current CPU usage on a single wc-client.


####### END OF ANALYSIS RUNNER

if __name__ == "__main__":
#    main()
#    txt2, t2 = model_2()
#    txt4, t4 = model_4()
#    log.info(txt2)
#    log.info(txt4)

#    test_model(model_1)
#    test_model(model_2)
#    test_model(model_3)
    test_model(model_4)

