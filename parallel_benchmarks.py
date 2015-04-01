import time
import multiprocessing as mp
import random

N = 10 # Blocking tasks to solve
P = 8 # Max processes
T = 2 # Max blocker duration (timeout)

def IO(*args, **kwargs):
    time.sleep(random.random() * T)

def CPU(*args, **kwargs):
    _ = [x**x for x in range(5500)]
    # Takes about 1s for me

RATIO = 5 # Ratio of IOs per CPU calls (must be int)
CPUs = 3

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
    
    return txt 

def model_2():
    # Multiprocessed Benchmark
    ## N Processes w/ Synchronous Calls
    start_time = time.time()
    pool = mp.Pool(P)
    n = len(pool.map(executer, [x.__name__ for x in TASKS]))
    total_time = time.time() - start_time

    txt = "===== model_2 =====\n"
    txt += "Clock Time: {}\n".format(total_time)
    txt += "  tasks: {}\n".format(n)
    txt += "  per task: {}\n".format(total_time / n)
    
    return txt     

def model_3():
    # Asyncronous Benchmark
    ## Single Process w/ Async Calls
    pass
    
def model_4():
    # Multiprocessed Asyncronous
    ## N Processes w/ Async Calls
    pass

    
def main():    
#    print model_1()
    print model_2()
#    print model_3()
    
    return



if __name__ == "__main__":
    main()
