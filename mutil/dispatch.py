# parse arguments while retaining quoted things as single fields
import shlex
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from loguru import logger


@dataclass
class DArg:
    """A dispatch argument with automatic type conversion and validation."""

    name: str
    convert: Callable[[Any], Any] | None = None
    verify: Callable[[Any], bool] | None = None

    # error message if validation fails
    errmsg: str | None = None

    # final converted / validated input value
    val: Any | None = None

    # self-documentation for parameter
    desc: str | None = None

    # if value missing, allow default. Only works for last elements currently.
    # TODO: enable named parameter lists too.
    default: Any = None

    def __post_init__(self) -> None:
        # Parameters can't be named "State" because it conflicts with the Op.state shared global.
        assert self.name != "state"

    def isRest(self) -> bool:
        """Check if this argument consumes all remaining arguments.

        Returns True if this is a final consume list, False otherwise."""
        return self.name[0] == "*" if self.name else False

    def usename(self) -> str:
        """Get the usable storage name for this argument.

        Automatically removes the leading '*' for rest arguments, otherwise
        just passes through the original argument name."""
        if self.isRest():
            return self.name[1:]

        return self.name

    def validate(self, x: Any) -> bool:
        """Attempt to convert and verify 'x' setting it to self.val on success.

        If both 'convert' and 'verify' pass (if they are defined), returns True.
        If neither are defined, returns True.
        If either are defined and any fail, returns False."""

        if x is None:
            self.val = self.default
            return True

        # Conversion requested? Convert.
        final = x
        if self.convert:
            try:
                final = self.convert(x)
            except:
                return False

        # Verification requested? Verify.
        if self.verify:
            try:
                if self.verify(final):
                    self.val = final
                else:
                    return False
            except:
                return False
        else:
            # No verification requested? Just set original.
            self.val = final

        return True


@dataclass
class Op:
    """Subclass Op for your custom operation logic.

    Only required attribute is the run method.

    A 'state' variable will be passed into each Op() instance
    in case you need to share service-wide metadata across
    all operations (account IDs, cached web sessions, etc).

    The original argument full text (unmodified) is in 'oargs' and
    upon instance creation, we split 'oargs' into 'args' on spaces
    to make the default case of space-delimited command arguments
    easier (retains matching quotes as a single argument entry too).

    You can also define an 'argmap' override to provide a list of
    arguments to automatically parse, convert, verify, and populate
    as instance variables.
    """

    # operations can have a pass-through state for accessing shared global
    # entities if users want to pass through applications state to every command.
    # Just don't name any argument DArg("state") or this will break.
    state: Any = None

    # Note: 'args__' and 'oargs__' are named differently so they don't conflict
    #       with user details if users request their own argument names
    #       of 'args' or 'oargs'.

    # original args before processing
    oargs__: str | None = None

    # split args
    args__: list[Any] = field(default_factory=list)

    def argmap(self) -> list[DArg]:
        """Map of positional arguments to instance variable names.

        If name begins with '*' then it consumes the remaining arguments into a list."""

        # default: just copy 'args' into 'margs'
        return [DArg("*margs")]

    def setup(self):
        """Automatically split original full string args into a list of args.

        Note: don't try to make setup() into a __post_init__() method because then
              every subclass would need to implement their own empty __post_init__()
              just to call super() here."""

        if self.oargs__:
            # split string arguments while maintaining quoted params as whole arguments
            self.args__.extend(shlex.split(self.oargs__))

        # use argmap to populate instance variables given an automatic specification
        amap: Final = self.argmap()

        # fail up front if required number of arguments is more than provided number of arguments
        # Allow '*' arguments to be empty:
        reduceByStar = int(amap[-1].isRest() if amap else False)
        lenDiff = len(self.args__) - (len(amap) - reduceByStar)

        # reject command if:
        #  - missing arguments
        #  - but only if all arguments not provided don't have a default.
        #  - i.e. allow missing arguments if all missing args have defaults.
        # NOTE: minor bug here if the command only has one argument, but it's a *star argument, then we can't detect "missing arguments."
        #       Perhaps we need an option to set *star arguments to required=True so we can require something there?
        if lenDiff < 0:
            # Check if missing arguments have defaults
            missing_args = amap[len(amap) + lenDiff :]
            if not all([arg.default for arg in missing_args]):
                # but don't fail if the last argument is a "consume rest as list" arguments,
                # meaning technically this command can also accept the '*' param being empty.
                logger.error(
                    "Not enough arguments provided! Expected: {}",
                    " ".join([a.name for a in amap]),
                )
                return False

        # Now iterate all expected arguments and extract them by index
        for idx, name in enumerate(amap):
            if name.isRest():
                # if name starts with asterisk, use the remaining args and report success
                if name.validate(self.args__[idx:]):
                    setattr(self, name.usename(), name.val)
                else:
                    logger.error(
                        "Failed validation of argument {} ({}){}",
                        idx,
                        self.args__[idx:],
                        f": {name.errmsg}" if name.errmsg else "",
                    )

                    return False
            else:
                # TODO: refactor this logic to be cleaner.
                # The purpose is "give argument to validator if argument exists, but if argument
                #                 doesn't exist, give validator None so it can potentially use a default."""
                # This is also where we could potentially use named parameters (instead of only position indexes)
                # if we want users to be able to specify "cmd 1 2 3" or "cmd a=1 c=3" if "b" has a default.
                if name.validate(self.args__[idx] if idx < len(self.args__) else None):
                    setattr(self, name.usename(), name.val)
                else:
                    logger.error(
                        "Failed validation of argument '{}' (position {}) ({}) because: {}",
                        name.name,
                        idx,
                        self.args__[idx],
                        name.errmsg,
                    )

                    return False

        return True

    async def run(self) -> Any:
        """Command/Operation implementation."""
        raise NotImplementedError(
            "Please provide your own run method for each operation"
        )


@dataclass
class Dispatch:
    """A self-contained auto-completing partial-prefix-matching commad interface.

    Dispatch automatically creates a lookup table of minimum length non-conflicting
    prefix strings of your commands.

    Also commands can each have their own argument format with validation and
    transformations before being handed off to your individual command worker
    methods."""

    # user input of either {name: callable} or {category: {name: callable}} mappings
    opcodes: Mapping[str, type[Op] | Mapping[str, type[Op]]]

    # post-processed version of 'opcodes' for actual executing of things
    # (a flat version of every dis-ambiguated command name without category details)
    totalOps: Mapping[str, type[Op]] = field(init=False)

    # exact full-name to operation mapping (always present for every command)
    # (used for matching full-name substring matches so commands aren't all
    #  intercepted by the prefix match system. i.e. if you have command "help" and "helper", before
    #  this check, you could never run "help" because it would be an ambiguous prefix match.)
    fullOps: Mapping[str, type[Op]] = field(init=False)

    cmdcompletion: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )

    cmds: list[str] = field(default_factory=list)
    cmdsByGroup: list[str | dict[str, frozenset[str]]] = field(default_factory=list)

    def __post_init__(self):
        """Cache all command names and generate the full minimal lookup map."""

        def minimizeOperationsTable():
            totalOp = {}
            fullOp = {}

            # Count all prefixes of all commands so we can
            # determine if we have conflicting prefixes.
            # (conflicting prefixes will have a count > 1)
            allCmds: dict[str, int] = Counter()

            def populateForKey(key):
                prefix = ""
                for char in key:
                    prefix += char

                    # Record this prefix as... existing
                    allCmds[prefix] += 1

                    # Also maintain a list of how each potentially
                    # conflicting prefix maps to potentially expanded
                    # command names
                    self.cmdcompletion[prefix].append(key)

            for key, val in self.opcodes.items():
                # if this is a nested namespace command set,
                # index the INNER commands, not the namespace description itself.
                if isinstance(val, dict):
                    cg = {key: frozenset(val.keys())}
                    self.cmdsByGroup.append(cg)

                    for kkey in val.keys():
                        fullOp[kkey] = val[kkey]
                        populateForKey(kkey)
                        self.cmds.append(kkey)
                else:
                    # else, we have an op itself at the top level.
                    fullOp[key] = val
                    populateForKey(key)
                    self.cmds.append(key)
                    self.cmdsByGroup.append(key)

            def nonconflict(key, val):
                prefix = ""
                for char in key:
                    prefix += char

                    # Add to substring ops only if:
                    #   - substring is unique and unambiguous
                    #   - or, is EXACT match from original command set
                    if allCmds[prefix] == 1 or (
                        prefix not in totalOp and prefix in self.opcodes
                    ):
                        totalOp[prefix] = val

            # Now put every non-conflicting prefix into dispatch table
            for key, val in self.opcodes.items():
                if isinstance(val, dict):
                    for kkey, vval in val.items():
                        nonconflict(kkey, vval)
                else:
                    nonconflict(key, val)

            # Replace user provided opcode table with our complete
            # non-conflicting dispatch lookup table.
            self.totalOps = totalOp
            self.fullOps = fullOp
            self.cmds = sorted(self.cmds)

        minimizeOperationsTable()

    async def runop(self, cmd: str, oargs: str | None = None, state: Any = None) -> Any:
        """Run requested command.

        Validates 'args' for 'cmd' (if validator provided) then runs
        command (if exists).

        If command doesn't exist, prints a relevant error message."""

        gethelp = False

        # check if this is just an early-stopping doc request
        if cmd.endswith("?"):
            gethelp = True
            cmd = cmd[:-1]

        # Check for exact match first, then fall back to prefix match
        op = self.fullOps.get(cmd) or self.totalOps.get(cmd)

        # Set wholecmd appropriately
        if cmd in self.fullOps:
            # Exact match - cmd is already the full command name
            wholecmd = cmd
        elif cmd in self.totalOps:
            # Prefix match - get the full command name
            wholecmd = self.cmdcompletion[cmd][0]
        else:
            # No match at all
            wholecmd = cmd

        if (not op) and (cmd in self.totalOps):
            # If partial name given, show full command too.
            # If full gommand already given, don't duplicate it.
            opdesc = f"{cmd} :: {wholecmd}" if cmd != wholecmd else wholecmd
            logger.error(
                "[{}] Operation exists but has no implementation!",
                opdesc,
                cmd,
                wholecmd,
            )

            return None

        def printhelp(long=True):
            logger.info(
                "Command: {} {}",
                wholecmd,
                " ".join(
                    [
                        (
                            f'[{a.name} default="{a.default}"]'
                            if a.default is not None
                            else f"[{a.name}]"
                        )
                        for a in iop.argmap()
                    ]
                ),
            )

            if long:
                # Only print doc string if it isn't the default repr() for the class
                if op.__doc__ and "state: Any = None" not in op.__doc__:
                    logger.info("{} :: {}", wholecmd, op.__doc__.strip())

        if op:
            # create operation instance from command name retrieved above
            iop = op(state, oargs)

            if gethelp:
                printhelp()
                return None

            validated = True  # optimism! assume success

            try:
                validated = iop.setup()
            except NotImplementedError:
                # if no argument validation provided, just skip
                # and assume arguments are okay.
                pass

            # RUN THE COMMAND if argument validation passed
            if validated:
                return await iop.run()

            # print generic error message
            logger.error("[{}] Argument validation failed: {}", wholecmd, oargs)

            # also print reminder of command arguments, but without full doc string.
            printhelp(long=False)

            # command failed
            return None

        complete = self.cmdcompletion.get(cmd)
        if complete:
            print("Completion choices:", ", ".join(complete))
        else:
            print("All commands:", ", ".join(self.cmds))

            # Show full command breakdown on just '?' request
            if gethelp:
                rest = []
                for row in self.cmdsByGroup:
                    if isinstance(row, dict):
                        for k, v in row.items():
                            print(f"{k}:\n\t-", "\n\t- ".join(sorted(v)))
                    else:
                        rest.append(row)

                if rest:
                    print("Other:\b\t-", "\n\t- ".join(sorted(rest)))

        return None
