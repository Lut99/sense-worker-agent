# WORKFLOW.py
#   by Lut99
#
# Created:
#   10 Jul 2024, 19:57:00
# Last edited:
#   11 Jul 2024, 19:58:56
# Auto updated?
#   Yes
#
# Description:
#   Defines workflows as the workflow agent understands them.
#

import abc
import os
import socket
import threading
import time
import yaml
from io import TextIOWrapper
from ipaddress import AddressValueError, IPv4Address
from typing import Any, Dict, List, Optional, Union

import lib.log as log


##### HELPERS #####
class Bytes:
    """
        Defines the parsing of human-friendly bytes.
    """

    amount: float

    def __init__(self, amount: float):
        """
            Constructor for the Bytes.

            # Arguments
            - `amount`: The number of bytes to store.
        """

        self.amount = amount

    def _parse(raw: str) -> Optional[Any]:
        """
            Attempts to parse some Bytes from a string.

            # Arguments
            - `raw`: The raw string to parse.

            # Returns
            A new Bytes on success, or else None.
        """

        # If it's just a number, then parse it as bytes
        raw = raw.strip()
        try:
            return Bytes(float(raw))
        except ValueError:
            pass

        # Else, see if it ends with something
        factor = 1.0
        if len(raw) >= 2 and raw[-2:] == "Kb":
            # Kilobits
            raw = raw[:-2]
            factor = 1000.0 / 8.0
        elif len(raw) >= 3 and raw[-3:] == "Kib":
            # Kibibits
            raw = raw[:-3]
            factor = 1024.0 / 8.0
        elif len(raw) >= 2 and raw[-2:] == "KB":
            # Kilobytes
            raw = raw[:-2]
            factor = 1000.0
        elif len(raw) >= 3 and raw[-3:] == "KiB":
            # Kibibytes
            raw = raw[:-3]
            factor = 1024.0
        elif len(raw) >= 2 and raw[-2:] == "Mb":
            # Megabits
            raw = raw[:-2]
            factor = 1000.0 * 1000.0 / 8.0
        elif len(raw) >= 3 and raw[-3:] == "Mib":
            # Mebibits
            raw = raw[:-3]
            factor = 1024.0 * 1024.0 / 8.0
        elif len(raw) >= 2 and raw[-2:] == "MB":
            # Megabytes
            raw = raw[:-2]
            factor = 1000.0 * 1000.0
        elif len(raw) >= 3 and raw[-3:] == "MiB":
            # Mebibytes
            raw = raw[:-3]
            factor = 1024.0 * 1024.0
        elif len(raw) >= 2 and raw[-2:] == "Gb":
            # Gigabits
            raw = raw[:-2]
            factor = 1000.0 * 1000.0 * 1000.0 / 8.0
        elif len(raw) >= 3 and raw[-3:] == "Gib":
            # Gibibits
            raw = raw[:-3]
            factor = 1024.0 * 1024.0 * 1024.0 / 8.0
        elif len(raw) >= 2 and raw[-2:] == "GB":
            # Gigabytes
            raw = raw[:-2]
            factor = 1000.0 * 1000.0 * 1000.0
        elif len(raw) >= 3 and raw[-3:] == "GiB":
            # Gibibytes
            raw = raw[:-3]
            factor = 1024.0 * 1024.0 * 1024.0
        elif len(raw) >= 1 and raw[-1:] == "b":
            # Bits (at the end to not confuse with larger amounts)
            raw = raw[:-1]
            factor = 1.0 / 8.0
        elif len(raw) >= 1 and raw[-1:] == "B":
            # Bytes (at the end to not confuse with larger amounts)
            raw = raw[:-1]
            factor = 1.0
        else:
            log.perror(f"Failed to parse '{raw}' as human-readable bytes")

        # Attempt to parse the remainder as a number
        raw = raw.strip()
        try:
            return Bytes(factor * float(raw))
        except ValueError:
            log.perror(f"Failed to parse '{raw}' as a number for human-readable bytes")
            return None

    def as_human_bytes(self) -> str:
        if self.amount >= 1000 * 1000 * 1000:
            return f"{self.amount / (1000 * 1000 * 1000)} GB"
        elif self.amount >= 1000 * 1000:
            return f"{self.amount / (1000 * 1000)} MB"
        elif self.amount >= 1000:
            return f"{self.amount / 1000} KB"
        else:
            return f"{self.amount} B"
    def as_human_bibi_bytes(self) -> str:
        if self.amount >= 1024 * 1024 * 1024:
            return f"{self.amount / (1024 * 1024 * 1024)} GiB"
        elif self.amount >= 1024 * 1024:
            return f"{self.amount / (1024 * 1024)} MiB"
        elif self.amount >= 1024:
            return f"{self.amount / 1024} KiB"
        else:
            return f"{self.amount} B"
    def as_human_bits(self) -> str:
        amount = 8 * self.amount
        if amount >= 1000 * 1000 * 1000:
            return f"{amount / (1000 * 1000 * 1000)} Gb"
        elif amount >= 1000 * 1000:
            return f"{amount / (1000 * 1000)} Mb"
        elif amount >= 1000:
            return f"{amount / 1000} Kb"
        else:
            return f"{amount} b"
    def as_human_bibi_bits(self) -> str:
        amount = 8 * self.amount
        if amount >= 1024 * 1024 * 1024:
            return f"{amount / (1024 * 1024 * 1024)} Gib"
        elif amount >= 1024 * 1024:
            return f"{amount / (1024 * 1024)} Mib"
        elif amount >= 1024:
            return f"{amount / 1024} Kib"
        else:
            return f"{amount} b"

    def __str__(self) -> str:
        return f"{self.amount} bytes"

class Duration:
    """
        Defines the parsing of human-friendly durations.
    """

    amount: float

    def __init__(self, amount: float):
        """
            Constructor for the Duration.

            # Arguments
            - `amount`: The number of seconds to store.
        """

        self.amount = amount

    def _parse(raw: str) -> Optional[Any]:
        """
            Attempts to parse some Duration from a string.

            # Arguments
            - `raw`: The raw string to parse.

            # Returns
            A new Duration on success, or else None.
        """

        # If it's just a number, then parse it as seconds
        raw = raw.strip()
        try:
            return Duration(float(raw))
        except ValueError:
            pass

        # Else, see if it ends with something
        factor = 1.0
        if len(raw) >= 1 and raw[-1:] == "s":
            # Seconds
            raw = raw[:-1]
            factor = 1.0
        elif len(raw) >= 3 and raw[-3:] == "sec":
            # Seconds
            raw = raw[:-3]
            factor = 1.0
        elif len(raw) >= 7 and raw[-7:] == "seconds":
            # Seconds
            raw = raw[:-7]
            factor = 1.0
        elif len(raw) >= 1 and raw[-1:] == "m":
            # Minutes
            raw = raw[:-1]
            factor = 60.0
        elif len(raw) >= 3 and raw[-3:] == "min":
            # Minutes
            raw = raw[:-3]
            factor = 60.0
        elif len(raw) >= 7 and raw[-7:] == "minutes":
            # Minutes
            raw = raw[:-7]
            factor = 60.0
        elif len(raw) >= 1 and raw[-1:] == "h":
            # Minutes
            raw = raw[:-1]
            factor = 60.0 * 60.0
        elif len(raw) >= 2 and raw[-2:] == "hr":
            # Minutes
            raw = raw[:-2]
            factor = 60.0 * 60.0
        elif len(raw) >= 5 and raw[-5:] == "hours":
            # Minutes
            raw = raw[:-5]
            factor = 60.0 * 60.0
        else:
            log.perror(f"Failed to parse '{raw}' as a human-readable duration")

        # Attempt to parse the remainder as a number
        raw = raw.strip()
        try:
            return Duration(factor * float(raw))
        except ValueError:
            log.perror(f"Failed to parse '{raw}' as a number for a human-readable duration")
            return None

    def as_human_duration(self) -> str:
        if self.amount >= 60.0 * 60.0:
            amount = self.amount / (60.0 * 60.0)
            return f"{amount} {'hour' if amount == 1.0 else 'hours'}"
        elif self.amount >= 60.0:
            amount = self.amount / 60.0
            return f"{amount} {'minute' if amount == 1.0 else 'minutes'}"
        else:
            return f"{self.amount} {'second' if self.amount == 1.0 else 'seconds'}"

    def __str__(self) -> str:
        return f"{self.amount} {'second' if self.amount == 1.0 else 'seconds'}"

class SourceInfo:
    """
        Collects information about where to send a `Flow`'s traffic from.
    """

    # The interface to send the flow through.
    interface: str
    # The source port for the connection.
    port: int

    def __init__(self, interface: str, port: int):
        """
            Constructor for the SourceInfo.

            # Arguments
            - `interface`: The interface to send the flow through.
            - `port`: The source port for the connection.
        """

        self.interface = interface
        self.port = port

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses a SourceInfo from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect the info's entries.

            # Returns
            A new SourceInfo, or `None` if we failed to parse it.
        """

        interface = None
        port = None
        for ty, fields in data.items():
            # We allow one of multiple node types
            if ty == "interface":
                # Expect a value of string
                if not isinstance(fields, str):
                    log.perror(f"\"interface\"-field in SourceInfo definition is not a string")
                    return None
                if interface is not None:
                    log.perror(f"Duplicate \"interface\"-definition in SourceInfo definition")
                    return None
                interface = fields
            elif ty == "port":
                # Expect a value of integer
                if not isinstance(fields, int):
                    log.perror(f"\"port\"-field in SourceInfo definition is not an integer")
                    return None
                if port is not None:
                    log.perror(f"Duplicate \"port\"-definition in SourceInfo definition")
                    return None
                port = fields
            else:
                log.pwarn(f"Unknown field '{ty}' for SourceInfo definition")
        if interface is None:
            log.perror(f"Missing \"interface\"-definition in SourceInfo definition")
            return None
        if port is None:
            log.perror(f"Missing \"port\"-definition in SourceInfo definition")
            return None

        # OK! Done
        return SourceInfo(interface, port)

    def fmt(self, indent: int = 0) -> str:
        """
            Formats a SourceInfo.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        res = f"{' ' * indent}SourceInfo:\n"
        indent += 4
        res += f"{' ' * indent}interface: {self.interface}\n"
        res += f"{' ' * indent}port: {self.port}\n"
        return res

    def __str__(self) -> str:
        return f"SourceInfo({self.interface}, {self.port})"

class TargetInfo:
    """
        Collects information about where to send a `Flow`'s traffic to.
    """

    # The target address for the connection.
    addr: IPv4Address
    # The target port for the connection.
    port: int

    def __init__(self, addr: Union[IPv4Address, List[int], str], port: int):
        """
            Constructor for the TargetInfo.

            # Arguments
            - `interface`: The interface to send the flow through.
            - `port`: The source port for the connection.
        """

        # Resolve the address
        if isinstance(addr, IPv4Address):
            self.addr = addr
        elif isinstance(addr, str):
            # Try to resolve the hostname first
            try:
                ip = socket.gethostbyname(addr)
            except socket.gaierror as e:
                log.perror(f"Failed to resolve '{addr}' as a hostname or IP address: {e}")
                exit(1)

            # Try to parse
            try:
                self.addr = ip
            except AddressValueError as e:
                log.perror(f"Failed to parse '{ip}' as an IP address: {e}")
                exit(1)
        else:
            self.addr = IPv4Address('.'.join([str(n) for n in addr]))

        # Set the port too
        self.port = port

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses a TargetInfo from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect the info's entries.

            # Returns
            A new TargetInfo, or `None` if we failed to parse it.
        """

        addr = None
        port = None
        for ty, fields in data.items():
            # We allow one of multiple node types
            if ty == "addr":
                # Expect a value of string
                if not isinstance(fields, str):
                    log.perror(f"\"addr\"-field in TargetInfo definition is not a string")
                    return None
                if addr is not None:
                    log.perror(f"Duplicate \"addr\"-definition in TargetInfo definition")
                    return None
                addr = fields
            elif ty == "port":
                # Expect a value of integer
                if not isinstance(fields, int):
                    log.perror(f"\"port\"-field in TargetInfo definition is not an integer")
                    return None
                if port is not None:
                    log.perror(f"Duplicate \"port\"-definition in TargetInfo definition")
                    return None
                port = fields
            else:
                log.pwarn(f"Unknown field '{ty}' for TargetInfo definition")
        if addr is None:
            log.perror(f"Missing \"addr\"-definition in TargetInfo definition")
            return None
        if port is None:
            log.perror(f"Missing \"port\"-definition in TargetInfo definition")
            return None

        # OK! Done
        return TargetInfo(addr, port)

    def fmt(self, indent: int = 0) -> str:
        """
            Formats a TargetInfo.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        res = f"{' ' * indent}TargetInfo:\n"
        indent += 4
        res += f"{' ' * indent}addr: {self.addr}\n"
        res += f"{' ' * indent}port: {self.port}\n"
        return res

    def __str__(self) -> str:
        return f"TargetInfo({self.addr}:{self.port})"





##### AUXILLARY #####
class Flow:
    """
        Represents some simulation of a service.

        This is the baseclass. Any children implement specific behaviours.
    """

    # A description of where to send this Flow's traffic from.
    source: SourceInfo
    # A description of where to send this Flow's traffic to.
    target: TargetInfo
    # Whether the flow has completed running in the background or not.
    done: bool

    def __init__(self, source: SourceInfo, target: TargetInfo):
        """
            Constructor for the Flow.

            # Arguments
            - `source`: A description of where to send this Flow's traffic from.
            - `target`: A description of where to send this Flow's traffic to.
        """

        self.source = source
        self.target = target
        self.done = False

    @abc.abstractmethod
    def spawn(self):
        """
            Starts running the flow on a separated thread.
        """

        raise NotImplementedError()

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses any of the flows from the given dictionary.
        """

        # Make our decision based on the kind-field
        if "kind" not in data:
            log.perror(f"Missing \"kind\"-field in flow definition")
            return None
        if not isinstance(data["kind"], str):
            log.perror(f"\"kind\"-field in flow definition is not a string")
            return None
        kind = data["kind"]

        # Now decide
        if kind == "TimedNoise":
            if (flow := TimedNoiseFlow._parse(data)) is not None:
                return flow
            else:
                log.perror(f"Failed to parse flow definition as a TimedNoiseFlow")
                return None
        else:
            log.perror(f"Unknown flow kind '{kind}'")
            return None

    @abc.abstractmethod
    def fmt(self, indent: int = 0) -> str:
        raise NotImplementedError()

class TimedNoiseFlow(Flow):
    """
        Defines Flow that simply sends a fixed rate of random bytes over bandwidth.
    """

    # The time to run the flow for, in seconds.
    time: Duration
    # The bandwidth of data to send, in bytes/sec.
    bandwidth: Bytes
    # The size of every packet, in bytes.
    chunk_size: Bytes

    def __init__(self, time: Duration, bandwidth: Bytes, chunk_size: Bytes, source: SourceInfo, target: TargetInfo):
        """
            Constructor for the TimedNoiseFlow.

            # Arguments
            - `time`: The time to sent traffic for.
            - `bandwidth`: The amount of bytes/sec to try and get out per second.
            - `chunk_size`: The size per packet.
            - `source`: A description of where to send this Flow's traffic from.
            - `target`: A description of where to send this Flow's traffic to.
        """

        Flow.__init__(self, source, target)

        # Don't forget to also store the time
        self.time = time
        self.bandwidth = bandwidth
        self.chunk_size = chunk_size

    def spawn(self):
        """
            Starts running the flow on a separated thread.
        """

        def flow():
            """
                The actual flow running on a background thread.
            """
            log.pinfo(f"Starting TimedNoiseFlow of {self.bandwidth.as_human_bibi_bytes()}/sec in chunks of {self.chunk_size.as_human_bibi_bytes()} to '{self.target.addr}:{self.target.port}' for {self.time.as_human_duration()}")

            # Start sending random UDP packets
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            start = time.time()
            while time.time() - start < self.time.amount:
                # Send a chunk
                chunk = os.urandom(int(self.chunk_size.amount))
                sock.send(chunk, (str(self.target.addr), self.target.port))

                # Wait the appropriate amount of time before sending the next chunk
                time.sleep(1.0 / (self.bandwidth / self.chunk_size))

            # Done
            self.done = True

        # Spawn a thread
        bg = threading.Thread(target=flow, args=(), daemon=True)
        bg.start()

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses a TimedNoiseFlow from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect the flow's entries.

            # Returns
            A new TimedNoiseFlow, or `None` if we failed to parse it.
        """

        time = None
        bandwidth = None
        chunk_size = None
        source = None
        target = None
        for ty, fields in data.items():
            # We allow one of multiple node types
            if ty == "time":
                # Expect a value of integer
                if not isinstance(fields, int) and not isinstance(fields, str):
                    log.perror(f"\"time\"-field in TimedNoiseFlow definition is not an integer or string")
                    return None
                if time is not None:
                    log.perror(f"Duplicate \"time\"-definition in TimedNoiseFlow definition")
                    return None
                time = Duration._parse(str(fields))
                if time is None:
                    log.perror(f"Failed to parse \"time\"-definition in TimedNoiseFlow definition")
                    return None
            elif ty == "bandwidth":
                # Expect a value of integer
                if not isinstance(fields, int) and not isinstance(fields, str):
                    log.perror(f"\"bandwidth\"-field in TimedNoiseFlow definition is not an integer or string")
                    return None
                if bandwidth is not None:
                    log.perror(f"Duplicate \"bandwidth\"-definition in TimedNoiseFlow definition")
                    return None
                bandwidth = Bytes._parse(str(fields))
                if bandwidth is None:
                    log.perror(f"Failed to parse \"bandwidth\"-definition in TimedNoiseFlow definition")
                    return None
            elif ty == "chunk_size":
                # Expect a value of integer
                if not isinstance(fields, int) and not isinstance(fields, str):
                    log.perror(f"\"chunk_size\"-field in TimedNoiseFlow definition is not an integer or string")
                    return None
                if chunk_size is not None:
                    log.perror(f"Duplicate \"chunk_size\"-definition in TimedNoiseFlow definition")
                    return None
                chunk_size = Bytes._parse(str(fields))
                if chunk_size is None:
                    log.perror(f"Failed to parse \"chunk_size\"-definition in TimedNoiseFlow definition")
                    return None
            elif ty == "source":
                if not isinstance(fields, dict):
                    log.perror(f"\"source\"-field in TimedNoiseFlow definition is not an object")
                    return None
                source = SourceInfo._parse(fields)
                if source is None:
                    log.perror(f"Failed to parse \"source\"-field in TimedNoiseFlow definition")
                    return None
            elif ty == "target":
                if not isinstance(fields, dict):
                    log.perror(f"\"target\"-field in TimedNoiseFlow definition is not an object")
                    return None
                target = TargetInfo._parse(fields)
                if target is None:
                    log.perror(f"Failed to parse \"target\"-field in TimedNoiseFlow definition")
                    return None
            elif ty != "kind":
                log.pwarn(f"Unknown field '{ty}' for TimedNoiseFlow definition")
        if time is None:
            log.perror(f"Missing \"time\"-definition in TimedNoiseFlow definition")
            return None
        if bandwidth is None:
            log.perror(f"Missing \"bandwidth\"-definition in TimedNoiseFlow definition")
            return None
        if chunk_size is None:
            log.perror(f"Missing \"chunk_size\"-definition in TimedNoiseFlow definition")
            return None
        if source is None:
            log.perror(f"Missing \"source\"-definition in TimedNoiseFlow definition")
            return None
        if target is None:
            log.perror(f"Missing \"target\"-definition in TimedNoiseFlow definition")
            return None

        # OK! Done
        return TimedNoiseFlow(time, bandwidth, chunk_size, source, target)

    def fmt(self, indent: int = 0) -> str:
        """
            Formats a TimedNoiseFlow.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        res = f"{' ' * indent}TimedNoiseFlow:\n"
        indent += 4
        res += f"{' ' * indent}time: {self.time.as_human_duration()}\n"
        res += f"{' ' * indent}bandwidth: {self.bandwidth.as_human_bibi_bytes()}\n"
        res += f"{' ' * indent}chunk_size: {self.chunk_size.as_human_bibi_bytes()}\n"
        res += self.source.fmt(indent)
        res += self.target.fmt(indent)
        return res





##### LIBRARY #####
class Node(abc.ABC):
    """
        Defines a baseclass for Workflow nodes.

        These are what build the workflow graph.
    """

    name: str
    incoming: List[Any]
    outgoing: List[Any]

    def __init__(self, name: str, incoming: Union[Any, List[Any]], outgoing: Union[Any, List[Any]]):
        """
            Constructor for the Node.

            # Arguments
            - `name`: Some name for this node for debugging purposes.
            - `incoming`: Any previous nodes that must be completed before this one is executed.
            - `outgoing`: Any further outgoing `Node`s that should be traversed
              once thatever this node does is completed.
        """

        # Resolve the `incoming` as a list
        if isinstance(incoming, Node):
            incoming = [incoming]
        # Resolve the `outgoing` as a list
        if isinstance(outgoing, Node):
            outgoing = [outgoing]

        # Now we can store everything
        self.name = name
        self.incoming = incoming
        self.outgoing = outgoing

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses any of the nodes from the given dictionary.
        """

        # if (node := StartNode._parse(data)) is not None:
        #     return node
        if (node := FlowNode._parse(data)) is not None:
            return node
        elif (node := EndNode._parse(data)) is not None:
            return node
        else:
            return None

    @abc.abstractmethod
    def fmt(self, indent: int = 0) -> str:
        raise NotImplementedError()

class StartNode(Node):
    """
        The start node of the workflow graph.
    """


    def __init__(self, name: str, outgoing: Union[Node, List[Node]]):
        """
            Constructor for the StartNode.

            # Arguments
            - `name`: Some name for this node for debugging purposes.
            - `outgoing`: Any further outgoing `Node`s that should be traversed
              once the `flow` has completed.
        """

        # Initialize the super-part first
        Node.__init__(self, name, [], outgoing)

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses a StartNode from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect a `start`-entry.

            # Returns
            A new StartNode, or `None` if we failed to parse it.
        """

        node = None
        for ty, fields in data.items():
            # We're only parsing start nodes
            if ty != "start" and type != "<start>":
                return None
            # Only parse the one though
            if node is not None:
                log.perror(f"Found duplicate '{ty}'-field")
                return None
            # Assert its fields are objects
            if not isinstance(fields, dict):
                log.perror(f"Body of '{ty}' is not an object")
                return None
            # Attempt to find the name
            if "name" not in fields:
                log.perror(f"Missing \"name\"-field for '{ty}'")
                return None
            name = fields["name"]

            # Find the next nodes
            if "next" not in fields:
                log.perror(f"Missing \"next\"-field for '{name}'")
                return None
            if not isinstance(fields["next"], list):
                log.perror(f"List of next-nodes of '{name}' is not an array")
                return None
            next = fields["next"]

            # Parse the fields as an ordered list of nodes
            node = StartNode(name, [])
            for i, nested in enumerate(next):
                if (nested := Node._parse(nested)) is not None:
                    # Inject this node to incoming nodes
                    nested.incoming.append(node)
                    node.outgoing.append(nested)
                else:
                    log.perror(f"Failed to parse node {i} part of '{name}'")
                    return None
            if len(node.outgoing) == 0:
                log.perror(f"Missing outgoing nodes for '{name}'")
                return None

        # OK! Done
        return node

    def fmt(self, indent: int = 0) -> str:
        """
            Formats a StartNode.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        res = f"{' ' * indent}Start:\n"
        for outgoing in self.outgoing:
            res += outgoing.fmt(4 + indent) + '\n'
        if len(self.outgoing) == 0:
            res += f"{' ' * (4 + indent)}<empty>\n"
        return res

class FlowNode(Node):
    """
        Defines a node that represents a flow.

        Multiple outgoing edges may be defined from a FlowNode.
    """

    flow: Flow
    outgoing: List[Node]

    def __init__(self, name: str, flow: Flow, incoming: Union[Node, List[Node]], outgoing: Union[Node, List[Node]]):
        """
            Constructor for the FlowNode.

            # Arguments
            - `name`: Some name for this node for debugging purposes.
            - `flow`: The `Flow` that is initiated when running this edge.
            - `incoming`: Any previous nodes that must be completed before this one is executed.
            - `outgoing`: Any further outgoing `Node`s that should be traversed
              once the `flow` has completed.
        """

        # Initialize the super-part first
        Node.__init__(self, name, incoming, outgoing)

        # Now store the flow
        self.flow = flow

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses an FlowNode from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect a `flow`-entry.

            # Returns
            A new FlowNode, or `None` if we failed to parse it.
        """

        node = None
        for ty, fields in data.items():
            # We're only parsing flow nodes
            if ty != "flow" and type != "<flow>":
                return None
            # Only parse the one though
            if node is not None:
                log.perror(f"Found duplicate '{ty}'-field")
                return None
            # Assert its fields are objects
            if not isinstance(fields, dict):
                log.perror(f"Body of '{ty}' is not an object")
                return None
            # Attempt to find the name
            if "name" not in fields:
                log.perror(f"Missing \"name\"-field for '{ty}'")
                return None
            name = fields["name"]

            # Find the flow
            if "flow" not in fields:
                log.perror(f"Missing \"flow\"-field for '{name}'")
                return None
            if not isinstance(fields["flow"], dict):
                log.perror(f"Flow-definition of '{name}' is not an object")
                return None
            flow = Flow._parse(fields["flow"])
            if flow is None:
                log.perror(f"Failed to parse flow definition of '{name}'")
                return None

            # Find the next nodes
            if "next" not in fields:
                log.perror(f"Missing \"next\"-field for '{name}'")
                return None
            if not isinstance(fields["next"], list):
                log.perror(f"List of next-nodes of '{name}' is not an array")
                return None
            next = fields["next"]

            # Parse the fields as an ordered list of nodes
            node = FlowNode(name, flow, [], [])
            for i, nested in enumerate(next):
                if (nested := Node._parse(nested)) is not None:
                    # Inject this node to incoming nodes
                    nested.incoming.append(node)
                    node.outgoing.append(nested)
                else:
                    log.perror(f"Failed to parse node {i} part of '{name}'")
                    return None
            if len(node.outgoing) == 0:
                log.perror(f"Missing outgoing nodes for '{name}'")
                return None

        # OK! Done
        return node

    def fmt(self, indent: int = 0) -> str:
        """
            Formats a FlowNode.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        res = f"{' ' * indent}Flow:\n"
        res += self.flow.fmt(4 + indent)
        for outgoing in self.outgoing:
            res += outgoing.fmt(4 + indent) + '\n'
        if len(self.outgoing) == 0:
            res += f"{' ' * (4 + indent)}<empty>\n"
        return res

class EndNode(Node):
    """
        Defines a node that terminates a branch of the workflow.
    """

    def __init__(self, name: str, incoming: Union[Node, List[Node]]):
        """
            Constructor for the EndNode.

            # Arguments
            - `name`: Some name for this node for debugging purposes.
            - `incoming`: Any previous nodes that must be completed before this one is executed.
        """

        # Initialize the super-part first
        Node.__init__(self, name, incoming, [])

    def _parse(data: Dict[str, Any]) -> Optional[Any]:
        """
            Parses an EndNode from a YAML/JSON dictionary.

            # Arguments
            - `data`: The dictionary where we expect a `start`-entry.

            # Returns
            A new EndNode, or `None` if we failed to parse it.
        """

        node = None
        for ty, fields in data.items():
            # We're only parsing end nodes
            if ty != "end" and type != "<end>":
                return None
            # Only parse the one though
            if node is not None:
                log.perror(f"Found duplicate '{ty}'-field")
                return None
            # Assert its fields are objects
            if not isinstance(fields, dict):
                log.perror(f"Body of '{ty}' is not an object")
                return None
            # Attempt to find the name
            if "name" not in fields:
                log.perror(f"Missing \"name\"-field for '{ty}'")
                return None
            name = fields["name"]

            # OK
            node = EndNode(name, [])

        # OK! Done
        return node

    def fmt(self, indent: int = 0) -> str:
        """
            Formats an EndNode.

            # Arguments
            - `indent`: The number of spaces to print before every line.
            
            # Returns
            The formatted string.
        """

        return f"{' ' * indent}End\n"



class Workflow:
    """
        Defines an abstract represention of some workflow.

        A workflow is here a definition of traffic flows, potentially ordered
        in time. The behaviour of every flow is defined in `Flow`; the
        `Workflow` merely orchestrates them.
    """

    # The workflow's name (for debugging)
    name: str
    # The first node
    graph: StartNode


    def __init__(self, name: str, graph: StartNode):
        """
            Constructor for the Workflow.

            # Arguments
            - `name`: Some name for the workflow so it's recognizable in debug logs. If you've read
              from a file, the filename might be a good choice.
            - `graph`: The workflow graph that this workflow represents. See `parse()` to read it
              from a file.
        """

        self.name = name
        self.graph = graph

    def parse_handle(name: str, handle: TextIOWrapper, fmt: str):
        """
            Parses a Workflow from a file.

            # Arguments
            - `name`: Some name for this workflow so it's recognizable in debug logs.
            - `handle`: Some `TextIOWrapper` that contains the raw source text to parse.
            - `fmt`: The format of the file. Can be 'yaml'.
        """

        # Load the file's contents
        try:
            raw = handle.read()
        except IOError as e:
            log.perror(f"Failed to read workflow '{name}': {e}")
            exit(e.errno)

        # Parse as the formatter
        if fmt == "yaml":
            data = yaml.load(raw, Loader=yaml.Loader)
        else:
            raise ValueError(f"Unknown workflow format '{fmt}'")
        if data is None:
            log.pwarn(f"Workflow '{name}' is empty (nothing to do)")
            return Workflow(name, StartNode("<start>", []))

        # Then parse as a start node
        graph = StartNode._parse(data)
        if graph is None:
            log.perror(f"Did not find a start node for workflow '{name}'")
            exit(1)

        # OK
        return Workflow(name, graph)



    def fmt(self, indent: int = 0) -> str:
        """
            Formats this workflow to summat readable.

            # Arguments
            - `indent`: The indentation to write before very line.

            # Returns
            A string that is this workflow but formatted.
        """

        return f"{' ' * indent}Workflow '{self.name}':\n" + self.graph.fmt(4 + indent)
