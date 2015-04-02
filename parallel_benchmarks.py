import time
import multiprocessing as mp
import random
import os
import threading
import Queue

MAX_PROCESSES = 8 
MAX_THREADS = 8
TIMEOUT = 2

def IO(*args, **kwargs):
    print "IO :: {}".format(os.getpid())
    time.sleep(random.random() * TIMEOUT)

def CPU(*args, **kwargs):
    print "CPU :: {}".format(os.getpid())
    _ = [x**x for x in range(5500)]
    # Takes about 1s for me

RATIO = 1 # Ratio of IOs per CPU calls (must be int)
CPUs = 5

_IOs = [IO for _ in range(RATIO * CPUs)]
TASKS = random.sample([CPU for _ in range(CPUs)] + _IOs, CPUs + RATIO * CPUs)

def executer(name):
    if name == 'CPU':
        return CPU()
    elif name == 'IO':
        return IO()
    
def model_1():
    # Sequential Benchmark
    ## Single Process w/ Synchronous Calls
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
    q = Queue.Queue()
    for t in TASKS:
        q.put(t)

    def target_func():
        task = q.get()
        while task:
            task()
            try:
                task = q.get()
                print task
            except Queue.Empty:
                task = None

    start_time = time.time()

    threads = []

    print " calling" 
    for _ in range(MAX_THREADS):
        thread = threading.Thread(target=target_func)
        thread.start()
        threads.append(thread)
    print " called" 
#    for t in threads:
#        t.join()
    print " joined" 
    n = len(TASKS)
    total_time = time.time() - start_time

    txt = "===== model_3 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)

    return txt, total_time / n    
    
def model_4():
    # Multiprocessed Asyncronous
    ## N Processes w/ Async Calls
    pass

    
def main():    
    txt1, t1 =  model_1()
    txt2, t2 =  model_2()

    print txt1
    print txt2

    print
    print "1 : {}".format(round(t2/t1, 4))
#    print model_3()
    
    return

if __name__ == "__main__":
    print model_3()
