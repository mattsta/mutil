from dataclasses import dataclass, field
from typing import *
import enum

# parse arguments while retaining quoted things as single fields
import shlex

from collections import Counter, defaultdict
from loguru import logger


@dataclass
class DArg:
    """A dispatch argument with automatic type conversion and validation."""

    name: str
    convert: Optional[Callable[[Any], Any]] = None
    verify: Optional[Callable[[Any], bool]] = None
    errmsg: Optional[str] = None  # error message if validation fails
    val: Optional[Any] = None  # final converted / validated input value
    desc: Optional[str] = None  # self-documentation for parameter

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

        If both pass (if they are defined), returns True.
        If neither are defined, returns True.
        If either are defined and any fail, returns False."""

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

    state: Any = None
    oargs: Optional[str] = None  # original args
    args: Iterable[Any] = field(default_factory=list)  # split args

    def argmap(self) -> list[DArg]:
        """Map of positional arguments to instance variable names.

        If name begins with '*' then it consumes the remaining arguments into a list."""

        # default: just copy 'args' into 'margs'
        return [DArg("*margs")]

    def setup(self):
        """Automatically split original full string args into a list of args"""

        if self.oargs:
            # split string arguments while maintaining quoted params as whole arguments
            self.args.extend(shlex.split(self.oargs))

        # use argmap to populate instance variables given an automatic specification
        amap = self.argmap()

        # fail up front if required number of arguments is more than provided number of arguments
        # Allow '*' arguments to be empty:
        reduceByStar = int(amap[-1].isRest() if amap else False)
        lenDiff = len(self.args) - (len(amap) - reduceByStar)
        if lenDiff < 0:
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
                if name.validate(self.args[idx:]):
                    setattr(self, name.usename(), name.val)
                else:
                    logger.error(
                        "Failed validation of argument {} ({}){}",
                        idx,
                        self.args[idx:],
                        f": {name.errmsg}" if name.errmsg else "",
                    )

                    return False
            else:
                if name.validate(self.args[idx]):
                    setattr(self, name.usename(), name.val)
                else:
                    logger.error(
                        "Failed validation of argument {} ({}) because: {}",
                        idx,
                        self.args[idx],
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

    opcodes: dict[str, Type[Op]]

    cmdcompletion: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def __post_init__(self):
        """Cache all command names and generate the full minimal lookup map."""
        self.cmds = []
        self.cmdsByGroup = []

        def minimizeOperationsTable():
            totalOp = {}

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

            # Count all prefixes of all commands so we can
            # determine if we have conflicting prefixes.
            # (conflicting prefixes will have a count > 1)
            allCmds = Counter()
            for key, val in self.opcodes.items():
                # if this is a nested namespace command set, index
                # the INNER commands, not the namespace description itself.
                if isinstance(val, dict):
                    cg = {key: val.keys()}
                    self.cmdsByGroup.append(cg)
                    for kkey in val.keys():
                        populateForKey(kkey)
                        self.cmds.append(kkey)
                else:
                    # else, we have an op itself at the top level.
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
            self.opcodes = totalOp
            self.cmds = sorted(self.cmds)

        minimizeOperationsTable()

    async def runop(
        self, cmd: str, oargs: Optional[str] = None, state: Any = None
    ) -> Any:
        """Run requested command.

        Validates 'args' for 'cmd' (if validator provided) then runs
        command (if exists).

        If command doesn't exist, prints a relevant error message."""

        gethelp = False

        # check if this is just an early-stopping doc request
        if cmd.endswith("?"):
            gethelp = True
            cmd = cmd[:-1]

        op = self.opcodes.get(cmd)

        if (not op) and (cmd in self.opcodes):
            wholecmd = self.cmdcompletion[cmd][0]

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

        if op:
            # create operation instance from command name retrieved above
            iop = op(state, oargs)

            # We already have a disambiguated command here, so we can retrieve
            # the full name directly.
            wholecmd = self.cmdcompletion[cmd][0]

            if gethelp:
                logger.info(
                    "Command: {} {}",
                    wholecmd,
                    " ".join([f"[{a.name}]" for a in iop.argmap()]),
                )

                # Only print doc string if it isn't the default repr() for the class
                if op.__doc__ and "state: Any = None" not in op.__doc__:
                    logger.info("{} :: {}", wholecmd, op.__doc__.strip())

                return None

            try:
                validated = True  # optimism! assume success
                validated = iop.setup()
            except NotImplementedError:
                # if no argument validation provided, just skip
                # and assume arguments are okay.
                pass

            if validated:
                return await iop.run()

            logger.error("[{}] Argument validation failed: {}", wholecmd, oargs)
            return None

        complete = self.cmdcompletion.get(cmd)
        if complete:
            print("Completion choices:", ", ".join(complete))
        else:
            print(f"All commands:", ", ".join(self.cmds))

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
                    print(f"Other:\b\t-", "\n\t- ".join(sorted(rest)))

        return None
