"""A loop for replacing consumed asyncio tasks with
new asyncio tasks as they partially complete.

This is needed because the asyncio.wait() interface
doesn't re-populate uncompleted tasks for the next wait
call, so we need to manipulate the re-addion of waiting
for still uncompleted tasks manually."""

import asyncio


class ForeverLoop:
    def __init__(self):
        self.allRunners = dict()

    def reset(self):
        """Delete all done items.

        Used when our state gets out of sync and we need an easy reset.

        Note: this can drop already done, but un-awaited results, so only
        use during error cleanups."""
        done = []
        for key in self.allRunners.keys():
            if key.done():
                done.append(key)

        for d in done:
            del self.allRunners[d]

        return done

    def addRunner(self, t, meta=None):
        # meta is: (fn to re-create context for t(), args for fn)
        # so, meta = (fn, args)
        # and, t = fn(args)
        self.allRunners[asyncio.create_task(t())] = (t, meta)

    def metaForTask(self, task):
        """Retrieve metadata so, most likely, caller can re-create task."""
        t, meta = self.allRunners[task]
        return meta

    def addRunners(self, ts):
        for t, meta in ts:
            self.addRunner(t, meta)

    def removeRunner(self, task):
        del self.allRunners[task]

    def removeRunnerSafe(self, task):
        if task in self.allRunners:
            del self.allRunners[task]

    def replaceRunner(self, task):
        t, meta = self.allRunners[task]
        del self.allRunners[task]
        self.addRunner(t, meta)

    async def getAllOnce(self):
        done, pending = await asyncio.wait(
            self.allRunners.keys(), timeout=0, return_when=asyncio.FIRST_COMPLETED
        )

        e = []
        for d in done:
            try:
                e.append(await d)
            except:
                # eat all exceptions but don't return them
                pass

            # Don't re-schedule this because we're removing all done tasks
            self.removeRunner(d)

        return e

    async def runForever(self, runners=[]):
        """Given a list of coroutine functions (un-executed, just
        the functions themselves), launch them all and await concurrently,
        returning each result from the generator, then re-schedule the
        completed coroutine as a new task."""

        # To easily replace tasks, we store the task as a key and the
        # function needed to re-create the task as the value.
        # Then we async wait on .keys() and re-populate using .values()

        self.addRunners(runners)

        while True:
            done, pending = await asyncio.wait(
                self.allRunners.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for d in done:
                # We don't *await* the iterable here because the caller
                # may need to run fl.removeRunner(d) itself if awaiting
                # for d throws an exception (connection error, timeout, etc)
                yield d
                self.replaceRunner(d)
