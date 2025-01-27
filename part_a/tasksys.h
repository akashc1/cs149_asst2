#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <atomic>
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
    private:
        int num_threads;
        std::vector<std::thread> threadpool;
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
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    private:
        int num_threads;
        std::vector<std::thread> threadpool;
    protected:
        // synchronize next index from tasks to work on
        std::mutex sync_mutex;
        int next_work_item = -1;

        // synchornize index of last completed item
        std::mutex done_mutex;
        int num_done_items = -1;

        // synchronize when we've finished all work for this object
        std::mutex start_mutex;
        bool finished = false;

        // synchronize when threads acknowledge that we're done working
        std::mutex waiting_mutex;
        int waiting_threads = -1;

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
        int num_threads;
        std::vector<std::thread> threadpool;
        //
        // synchronize on the next item to work on
        std::mutex m;
        int next_work_id;

        // synchronize when threads are waiting/sleeping
        std::atomic<int> num_waiting_threads;
        std::condition_variable cv;

        // sychronize on master sleeping
        std::mutex master_mutex;
        std::condition_variable master_cv;

        // state of task queue
        IRunnable* tasks;
        int num_tasks;

        // exit flag
        bool finished = false;

        // main thread worker loop
        void run_sleeping(int thread_id);

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
