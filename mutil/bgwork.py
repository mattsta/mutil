#!/usr/bin/env python3

import multiprocessing
import os
import queue
from collections.abc import Callable
from typing import Any

from loguru import logger


@logger.catch
def _process_task_executor(
    task_queue: multiprocessing.JoinableQueue, task_function: Callable[..., Any]
):
    """
    Worker function that processes tasks from a queue.
    Each task is a tuple of arguments for task_function.
    A None task signals the worker to terminate.
    """
    while True:
        try:
            task_args: tuple[Any, ...] | None = task_queue.get()

            if task_args is None:  # Sentinel value to signal termination
                task_queue.task_done()
                logger.debug("Worker process received sentinel, exiting.")
                break

            try:
                task_function(*task_args)
            except Exception:
                logger.exception(
                    "Unhandled exception in background task execution for target: {}.",
                    getattr(task_function, "__name__", "unknown_function"),
                )
            finally:
                task_queue.task_done()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Worker process exiting due to KeyboardInterrupt/SystemExit.")
            # Ensure task_done is called if a task was fetched before interruption
            # This path is tricky; if get() was interrupted, task_done isn't needed.
            # If interruption happened during task_function, it's already handled.
            # Best to break and let the main process handle queue joining issues if any.
            break
        except queue.Empty:
            # This should ideally not happen with a blocking get() unless the queue
            # is closed unexpectedly or sentinel logic has issues.
            logger.warning("Worker process found queue empty unexpectedly, exiting.")
            break
        except Exception:
            logger.exception("Unexpected error in worker process loop. Exiting.")
            break


class BackgroundProcessPool:
    """
    A context manager for running a target function in background processes.

    Manages a pool of worker processes that consume tasks from a queue.
    Tasks are submitted as tuples of arguments for the target function.

    Usage:
        def my_task_function(arg1, arg2):
            # ... do work ...
            logger.info(f"Processing {arg1}, {arg2}")

        with BackgroundProcessPool(my_task_function, cpu_multiple=0.5) as submit_task:
            if submit_task is not None: # Check if submit_task is not a no-op
                submit_task("hello", 1)
                submit_task("world", 2)
        # All tasks are processed before exiting the 'with' block.
    """

    def __init__(
        self,
        target_function: Callable[..., Any] | None,
        cpu_multiple: float = 1.0,
        max_workers: int | None = None,
    ):
        """
        Initializes the BackgroundProcessPool.

        Args:
            target_function: The function to be executed by worker processes.
                             If None, the pool will be a no-op.
            cpu_multiple: Multiplier for os.cpu_count() to determine # of workers.
                          Default is 1.0. Ignored if max_workers is set.
            max_workers: Specific number of worker processes. Overrides cpu_multiple.
        """
        self.target_function = target_function
        self.cpu_multiple = cpu_multiple
        self.max_workers = max_workers

        self._task_queue: multiprocessing.JoinableQueue | None = None
        self._processes: list[multiprocessing.Process] = []
        self._noop_submit = lambda *args, **kwargs: None

    def __enter__(self) -> Callable[..., None]:
        """
        Enters the runtime context, starting worker processes.
        Returns a function to submit tasks to the pool.
        """
        if self.target_function is None:
            logger.debug(
                "No target function provided; BackgroundProcessPool will be a no-op."
            )
            return self._noop_submit

        self._task_queue = multiprocessing.JoinableQueue()

        if self.max_workers is not None:
            num_processes = self.max_workers
        else:
            num_processes = round((os.cpu_count() or 4) * self.cpu_multiple)

        num_processes = max(1, num_processes)  # Ensure at least one process

        for _ in range(num_processes):
            process = multiprocessing.Process(
                target=_process_task_executor,
                args=(self._task_queue, self.target_function),
                daemon=True,  # Daemon processes auto-terminate if main process exits
            )
            process.start()
            self._processes.append(process)
            logger.debug(
                "Spawned worker process: {} (PID: {}) for target: {}",
                process.name,
                process.pid,
                getattr(self.target_function, "__name__", "unknown_function"),
            )

        logger.opt(depth=1).info(
            "BackgroundProcessPool started with {} worker process(es) for target: {}.",
            num_processes,
            getattr(self.target_function, "__name__", "unknown_function"),
        )

        return self._task_queue.put

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Exits the runtime context, ensuring all tasks are processed and workers shut down.
        """
        if not self._processes or not self._task_queue:
            logger.debug("BackgroundProcessPool exiting (no-op or not started).")
            return

        logger.debug(
            "BackgroundProcessPool shutting down. Signaling workers to exit..."
        )

        for _ in self._processes:
            self._task_queue.put(None)  # Send sentinel value to each worker

        logger.debug("Waiting for all tasks to be processed...")
        self._task_queue.join()  # Wait for all tasks (including sentinels)
        logger.debug("All tasks processed.")

        logger.debug("Joining worker processes...")
        for process in self._processes:
            process.join(timeout=5)  # Wait for process to finish
            if process.is_alive():
                logger.warning(
                    "Worker process {} (PID: {}) did not exit cleanly after sentinel and timeout, terminating.",
                    process.name,
                    process.pid,
                )
                process.terminate()  # Force terminate if join times out
                process.join()  # Wait for termination to complete
            logger.debug(
                "Joined worker process: {} (PID: {})", process.name, process.pid
            )

        self._task_queue.close()
        self._task_queue.join_thread()  # Wait for queue's internal thread

        logger.info(
            "BackgroundProcessPool for target {} shut down successfully.",
            getattr(self.target_function, "__name__", "unknown_function"),
        )

        # Clean up internal state
        self._processes = []
        self._task_queue = None

        # Optional: Check for remaining active children for debugging
        # active_children = multiprocessing.active_children()
        # if active_children:
        #     logger.warning("Remaining active child processes after shutdown: {}", active_children)
