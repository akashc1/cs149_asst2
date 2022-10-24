#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <iostream>
#include <vector>
#include <condition_variable>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * LockedCounter: counter protected by a mutex for safe writes/reads.
 * */
class LockedCounter {
    public:
        /* LockedCounter(); */
        /* ~LockedCounter(); */

        int get_incremented_count();
        int get_count();
        void increment();
        void set_value(int val);

    private:
        std::mutex m;
        int counter = 0;
};

class LockedFlag {
    public:
        void set_true();
        void set_false();
        bool value();

    private:
        std::mutex m;
        bool flag = false;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    protected:
        /* // synchronize next index from tasks to work on */
        /* LockedCounter next_work_counter; */
        /* // synchronize on index of last completed item */
        /* LockedCounter done_work_counter; */
        /* // synchronize on which threads have acknowledge termination */
        /* LockedCounter sync_counter; */
        /* // synchornize on whether we should stop checking for more work to do */
        /* LockedFlag terminate_flag; */

        /* int next_work_item; */
        /* int completed_work_count; */
        /* bool terminate = false; */

        /* // synchronize when we should pause/resume working on some work */
        /* std::mutex work_lock; */
        /* std::mutex completed_lock; */
        /* std::mutex continue_mutex; */

        // actual tasks & count to work on for a singular call to run()
        IRunnable* tasks;
        int num_tasks;

    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    private:
        // manage which tasks to work on
        IRunnable* tasks;
        int num_tasks;

        // manage sleeping/waking behavior
        std::condition_variable _cv;
        std::mutex _m;
        int ack_count;

        // manage which item each thread should work on next, and checking for completion
        int next_work_item = 0;
        int num_completed_items = 0;
        std::mutex next_item_lock;
        std::mutex last_done_lock;

        // manage global state of whether we can tear down the queue
        bool terminate = false;

    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
