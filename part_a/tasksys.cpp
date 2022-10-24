#include "tasksys.h"
#include <condition_variable>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) : num_threads(num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

void thread_run(int task_num, IRunnable* runnable, int num_total_tasks, int num_workers) {
    for (int i = task_num; i < num_total_tasks; i += num_workers) {
        runnable->runTask(i, num_total_tasks);
    }
}

void thread_run_queue(IRunnable* runnable, int num_total_tasks, int* work_counter, std::mutex *m) {
    int work_idx;
    while (true) {
        (*m).lock();
        work_idx = ++(*work_counter);
        (*m).unlock();
        if (work_idx >= num_total_tasks) {
            return;
        }
        runnable->runTask(work_idx, num_total_tasks);
    }
}

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    int num_workers = std::min(num_threads, num_total_tasks);
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(thread_run, i, runnable, num_total_tasks, num_workers);
    }

    // Join threads
    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */
void LockedCounter::set_value(int val) {
    counter = val;
}

int LockedCounter::get_incremented_count() {
    int count;
    m.lock();
    count = ++counter;
    m.unlock();

    return count;
}

int LockedCounter::get_count() {
    int count;
    m.lock();
    count = counter;
    m.unlock();
    return count;
}

void LockedCounter::increment() {
    m.lock();
    ++counter;
    m.unlock();
}

void LockedFlag::set_true() {
    m.lock();
    flag = true;
    m.unlock();
}

void LockedFlag::set_false() {
    m.lock();
    flag = false;
    m.unlock();
}

bool LockedFlag::value() {
    bool _flag;
    m.lock();
    _flag = flag;
    m.unlock();
    return _flag;
}

void thread_run_queue_spin(IRunnable** runnable, int* num_total_tasks,
        LockedCounter* work_counter,
        LockedCounter* finished_counter,
        LockedCounter* sync_counter,
        LockedFlag* terminate_flag,
        std::mutex* continue_mutex) {

    int next_work_idx;
    while (!terminate_flag->value()) {
        continue_mutex->lock();  // ensure we can work
        continue_mutex->unlock();
        next_work_idx = work_counter->get_count();
        if (next_work_idx < *num_total_tasks
                && (next_work_idx = work_counter->get_incremented_count()) < *num_total_tasks) {
            (*runnable)->runTask(next_work_idx, *num_total_tasks);
            finished_counter->increment();
        }
    }

    sync_counter->increment();
}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads):
    ITaskSystem(num_threads) {
    /* threadpool = std::vector<std::thread>(num_threads); */
    /* continue_mutex.lock(); */
    /* for (int i = 0; i < num_threads; i++) { */
    /*     threadpool[i] = std::thread(thread_run_queue_spin, */
    /*             &tasks, &num_tasks, */
    /*             nullptr, */
    /*             nullptr, */
    /*             nullptr, */
    /*             &terminate_flag, */
    /*             &continue_mutex); */
    /* } */
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    /* // indicate to threads that we're ready to terminate */
    /* // here, we already have `start_mutex` locked */
    /* terminate_flag.set_true(); */
    /* continue_mutex.unlock(); */

    /* // wait for threads to acknowledge termination flag */
    /* while (sync_counter.get_count() < num_threads) {} */

    /* for (int i = 0; i < num_threads; i++) { */
    /*     threadpool[i].join(); */
    /* } */
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    /* // set values that all threads are watching via pointers to these */
    /* tasks = runnable; */
    /* num_tasks = num_total_tasks; */
    /* next_work_counter.set_value(-1); */
    /* done_work_counter.set_value(0); */
    /* continue_mutex.unlock();  // let threads begin work on this */

    /* // check if we've completed all items */
    /* int counter, next_work_item; */
    /* while (true) { */
    /*     next_work_item = next_work_counter.get_count(); */
    /*     counter = done_work_counter.get_count(); */
    /*     /1* printf("Next work index: %d\tNum finished: %d\tTotal: %d\n", *1/ */
    /*     /1*         next_work_item, counter,  num_total_tasks); *1/ */
    /*     if (counter >= num_total_tasks) { */
    /*         break; */
    /*     } */
    /* } */

    /* continue_mutex.lock(); */
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

void do_workload(IRunnable* tasks, int num_tasks,
        int* next_work_item, std::mutex* next_item_lock,
        int* num_completed_items, std::mutex* last_done_lock) {

    int work_idx;
    next_item_lock->lock();
    while ((work_idx = *next_work_item) < num_tasks) {
        (*next_work_item)++;
        next_item_lock->unlock();

        printf("Working on item %d / %d\n", work_idx, num_tasks);
        tasks->runTask(work_idx, num_tasks);
        last_done_lock->lock();
        (*num_completed_items)++;
        last_done_lock->unlock();

        next_item_lock->lock();
    }
    next_item_lock->unlock();
}

void thread_work_sleep(IRunnable* tasks, int* num_tasks,
        std::mutex* lock, std::condition_variable* cv, int* ack_count,
        int* next_work_item, std::mutex* next_item_lock,
        int* num_completed_items, std::mutex* last_done_lock,
        bool* terminate) {

    while (true) {
        // wait until indicated we should continue
        std::unique_lock<std::mutex> l(*lock);
        cv->wait(l);
        (*ack_count)++;  // ack that we've woken up
        printf("This is the %dst thread to be woken up!\tTerminate:%d\n", *ack_count, *terminate);
        l.unlock();

        if (*terminate) {
            return;
        }

        do_workload(tasks, *num_tasks, next_work_item, next_item_lock,
                num_completed_items, last_done_lock);
    }
}

void thread_work_single_work(IRunnable* tasks, int* num_tasks,
        std::mutex* lock, std::condition_variable* cv, int* ack_count,
        int* next_work_item, std::mutex* next_item_lock,
        int* num_completed_items, std::mutex* last_done_lock,
        bool* terminate) {

    while (true) {
        // wait until indicated we should continue
        std::unique_lock<std::mutex> l(*lock);
        cv->wait(l);
        (*ack_count)++;  // ack that we've woken up
        printf("This is the %dst thread to be woken up!\tTerminate:%d\n", *ack_count, *terminate);
        l.unlock();

        if (*terminate) {
            return;
        }

        int work_idx;
        next_item_lock->lock();
        if ((work_idx = *next_work_item) < *num_tasks) {
            (*next_work_item)++;
            next_item_lock->unlock();

            printf("Working on item %d / %d\n", work_idx, *num_tasks);
            tasks->runTask(work_idx, *num_tasks);
            printf("Finished working on item %d / %d\n", work_idx, *num_tasks);
            last_done_lock->lock();
            printf("Acquired lock to increment num_completed_items!\n");
            (*num_completed_items)++;
            printf("Incremented num_completed_items!\n");
            last_done_lock->unlock();
            printf("Incremented number of done items!\n");
        }
    }
}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    _m.lock();
    for (int i = 0; i < 1; i++) {
        threadpool[i] = std::thread(thread_work_single_work,  // thread_work_sleep,
                tasks, &num_tasks,
                &_m, &_cv, &ack_count,
                &next_work_item, &next_item_lock,
                &num_completed_items, &last_done_lock,
                &terminate);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    terminate = true;
    ack_count = 0;
    while (ack_count < num_threads) {
        _m.unlock();
        _cv.notify_all();
        _m.lock();
    }
    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    tasks = runnable;
    num_tasks = num_total_tasks;
    num_completed_items = 0;
    next_work_item = 0;
    ack_count = 0;
    int _ack_count = ack_count;
    while (ack_count < num_threads) {
        _ack_count = ack_count;
        _m.unlock();
        _cv.notify_all();
        _m.lock();
        printf("%d / %d threads have been woken up!\n", _ack_count, num_threads);
    }
    printf("All threads have acknowledged that we can do work!");
    while (num_completed_items < num_total_tasks) {}
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
