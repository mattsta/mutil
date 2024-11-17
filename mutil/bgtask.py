"""A helper for properly running asyncio tasks plus some extra features."""

import asyncio
import datetime
import inspect
import pprint

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Coroutine, Final, Mapping

from loguru import logger


@dataclass(slots=True)
class BGSchedule:
    # You can request an event start AT OR AFTER 'start'
    start: datetime.datetime | None = None

    # You can request an event stops spawning new events AT OR AFTER 'stop'
    stop: datetime.datetime | None = None

    # 'delay' seconds is added to 'start'.
    # If 'start' is not provided, 'start' becomes 'now + delay'
    delay: float = 0

    # You can request a task repeats multiple times.
    # Note: repeated tasks must provide a function and not a coroutine as the runner.
    runtimes: int = 1

    # You can request a task pauses for seconds between repeat runs.
    pause: float = 0

    # Internal accounting fields
    created: datetime.datetime = field(init=False)
    started: list[datetime.datetime] = field(default_factory=list)
    stopped: list[datetime.datetime] = field(default_factory=list)

    # Total run count so far
    runcount: int = 0

    # default timezone for objects
    tz: datetime.tzinfo = datetime.timezone.utc

    def __post_init__(self) -> None:
        self.created = self.now()

        if not self.start:
            self.start = self.created

        if self.delay:
            self.start += datetime.timedelta(self.delay)

    def now(self):
        return datetime.datetime.now(self.tz)


@dataclass(slots=True)
class BGTask:
    # Unlike regular Task objects, we *require* a name for every task.
    name: str

    # Something to run async. Can be a function we `await run()` or an awaitable
    # we directly `await run` instead.
    coroutine: Callable[..., Any] | Awaitable[Any]

    # Default schedule is empty (run now, run once, stop), but you can also
    # provide a custom schedule on startup with delay/runtimes/pause parameters.
    schedule: BGSchedule = field(default_factory=BGSchedule)

    # Task is our wrapper running the user's coroutine.
    # Using the good practice of "keep a reference to your Task so it doesn't get GC'd"
    task: asyncio.Task = field(init=False, repr=False)

    # Callable references to run when the task is completed.
    final: set[Callable[..., Any]] = field(default_factory=set, repr=False)

    # allow user to attach custom metadata to task for reporting
    meta: Any = None

    def __post_init__(self) -> None:
        # We wrap the user's requested coroutine in our own time management and accounting logic.
        async def starter():
            active = None

            try:
                if inspect.iscoroutinefunction(self.coroutine):
                    CREATE_COROUTINE = True
                else:
                    CREATE_COROUTINE = False
                    assert (
                        self.schedule.runtimes <= 1
                    ), f"For repeating events, you must provide a coroutine _function_ instead of a single coroutine"
                    active = self.coroutine

                runAfter = (self.schedule.start - self.schedule.now()).total_seconds()
                if runAfter > 0:
                    await asyncio.sleep(runAfter)

                for _ in range(self.schedule.runtimes):
                    try:
                        # record start time
                        now = self.schedule.now()
                        self.schedule.started.append(now)

                        # if the current time is at or beyond the stop time, don't run anymore.
                        # Note: the stop time only prevents _launching_ but it doesn't actually
                        #       terminate any in-progress coroutines if they run beyond 'stop' time.
                        if (stop := self.schedule.stop) and stop >= now:
                            break

                        # determine how to use coroutine for user request
                        if CREATE_COROUTINE:
                            # Note: for repeating events, callable must be a function so we create a new coroutine on each repeat.
                            active = self.coroutine()

                        # await the correct coroutine directly
                        await active
                    except Exception as e:
                        self.schedule.pause = 0
                        logger.exception("[{}] Exception: {}", self.name, str(e))
                        raise
                    finally:
                        self.schedule.stopped.append(self.schedule.now())
                        self.schedule.runcount += 1

                        if self.schedule.pause:
                            await asyncio.sleep(self.schedule.pause)
            finally:
                # if anything happens, we need to tell python the
                # created coroutine is officially consumed so it doesn't
                # report "coroutine was never awaited" if we never scheduled it.
                # (e.g. we schedule a delayed task, but canceled the delayed task before
                #       the coroutine executed, so now we have a floating "never awaited"
                #       coroutine unless we manually .close() it (and repeated .close() events
                #       even on a completed/closed coroutine works without errors)
                if active:
                    active.close()

        # Creating a BGTask instantly runs the task (or, at least starts the BGSchedule for the task)
        self.task = asyncio.create_task(starter(), name=self.name)
        self.task.add_done_callback(self.task_done)

    def __hash__(self) -> int:
        # We need this object to remain unique even as coroutines and tasks change over time.
        return hash((self.name, self.created))

    @property
    def created(self) -> datetime.datetime:
        return self.schedule.created

    @property
    def started(self) -> list[datetime.datetime]:
        return self.schedule.started

    @property
    def stopped(self) -> list[datetime.datetime]:
        return self.schedule.stopped

    def add_done_callback(self, cb) -> None:
        """When asyncio reports task is done, run these callbacks."""
        self.final.add(cb)

    def task_done(self, task) -> None:
        # for each done callback requested, run it...
        for closer in self.final:
            closer(self)

    def stop(self) -> None:
        if self.task and not self.task.done():
            # logger.info("Cancelling active task: {}", self.task)
            self.task.cancel()

    def __await__(self):
        return self.task.__await__()

    def __del__(self) -> None:
        self.stop()
        # logger.info("[{}] Deleting task", self.name)

    def report(self) -> None:
        meta = f" :: [{self.meta}] " if self.meta else ""
        logger.info(
            "[{} :: {}{} :: {}] {}",
            self.created,
            self.name,
            meta,
            id(self),
            self.coroutine,
        )
        logger.info(
            "[{} :: {}{}] Schedule: {}",
            self.created,
            self.name,
            meta,
            pprint.pformat(self.schedule),
        )
        logger.info(
            "[{} :: {}{}] Started: {} Stopped: {}",
            self.created,
            self.name,
            meta,
            self.started,
            self.stopped,
        )
        logger.info(
            "[{} :: {}{}] Durations: {}",
            self.created,
            self.name,
            meta,
            [stop - start for start, stop in zip(self.started, self.stopped)],
        )


@dataclass(slots=True)
class BGTasksAvailable:
    """A custom mapping of task requests to how we should run them.

    Purpose: if we want to save schedules across restarts, we can't just pickle
             coroutines across restarts, so we need a persistent custom mapping
             of task requests to underlying functions or coroutines to run too.

    Design idea:
      - task request string name ("echo")
      - maps to a function (Echo.echo)
      - arugments can be saved and restored (cloudpickle)
      - so we can create persistent tasks _across restarts_ which can
        remember their arguments and resume running.
    """

    requestTarget: Mapping[str, Callable[..., Any]] = field(default_factory=dict)


@dataclass(slots=True)
class BGTasks:
    # name for this collective task scheduler.
    # name should be unique across your platforms because in the future
    # we may use this name to generate automatic task caching for reloading
    # events across restarts.
    name: str

    tasks: Final[set[BGTask]] = field(default_factory=set)

    # call something when a task has been removed
    final: Callable[..., Any] | None = None

    def create(self, *args, **kwargs) -> BGTask:
        task = BGTask(*args, **kwargs)
        self.tasks.add(task)
        task.add_done_callback(self.cleanup)
        return task

    def stop(self, task):
        return task.stop()

    def stopId(self, taskId: int):
        # Lazy O(N) iterator here because we aren't indexing tasks by anything
        # useful and currently we expect to have less than 5-10 tasks active at once.
        # If this is a larger concern, we could maintain an id-index-to-task map too.
        for task in self:
            if id(task) == taskId:
                return task.stop()

    def cleanup(self, task) -> None:
        if self.final:
            self.final()

        self.tasks.remove(task)

    def __len__(self) -> int:
        return len(self.tasks)

    def __iter__(self):
        yield from sorted(self.tasks, key=lambda x: x.schedule.created)

    def cancel(self, name):
        removed = set()
        for task in self.tasks.copy():
            if task.name == name:
                removed.add(task)

                # Note: STOPPING the task REMOVES it from 'self.tasks' here,
                #       so _do not_ self.tasks.remove() here too.
                task.stop()

        return removed

    def report(self) -> None:
        for task in self:
            task.report()
