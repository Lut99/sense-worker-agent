# WORKFLOW.py
#   by Lut99
#
# Created:
#   10 Jul 2024, 19:57:00
# Last edited:
#   10 Jul 2024, 22:42:02
# Auto updated?
#   Yes
#
# Description:
#   Defines workflows as the workflow agent understands them.
#

import abc
import socket
import yaml
from io import TextIOWrapper
from ipaddress import AddressValueError, IPv4Address
from typing import Any, Dict, List, Optional, Union

import lib.log as log


##### LIBRARY #####
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



class Flow:
    """
        Represents some simulation of a service.

        This is the baseclass. Any children implement specific behaviours.
    """

    # A description of where to send this Flow's traffic from.
    source: SourceInfo
    # A description of where to send this Flow's traffic to.
    target: TargetInfo

    def __init__(self, source: SourceInfo, target: TargetInfo):
        """
            Constructor for the Flow.

            # Arguments
            - `source`: A description of where to send this Flow's traffic from.
            - `target`: A description of where to send this Flow's traffic to.
        """

        self.source = source
        self.target = target

    @abc.abstractmethod
    def fmt(self, indent: int = 0) -> str:
        raise NotImplementedError()

class TimedNoiseFlow(Flow):
    """
        Defines Flow that simply sends a fixed rate of random bytes over bandwidth.
    """

    # The time to run the flow for, in seconds.
    time: int

    def __init__(self, time: int, source: SourceInfo, target: TargetInfo):
        """
            Constructor for the TimedNoiseFlow.

            # Arguments
            - `time`: The time to sent traffic for.
            - `source`: A description of where to send this Flow's traffic from.
            - `target`: A description of where to send this Flow's traffic to.
        """

        Flow.__init__(self, source, target)

        # Don't forget to also store the time
        self.time = time

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
        res += f"{' ' * indent}time: {self.time}s\n"
        res += self.source.fmt(indent)
        res += self.target.fmt(indent)
        return res



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

        if (node := StartNode._parse(data)) is not None:
            return node
        elif (node := FlowNode._parse(data)) is not None:
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

        # Parse the thing
        if 'start' in data:
            name = "start"
            body = data['start']
        elif '<start>' in data:
            name = "<start>"
            body = data['<start>']
        else:
            return None

        # Assert it contains a list
        if not isinstance(body, list):
            log.perror(f"'{name}'-field is not an ordered list")
            return None
        outgoing = []
        for data in body:
            if (node := Node._parse(data)) is not None:
                outgoing.append(node)
            else:
                log.perror(f"Failed to parse node in '{name}'-list")
                return None
        return StartNode(name, outgoing)

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
            - `data`: The dictionary where we expect a `start`-entry.

            # Returns
            A new FlowNode, or `None` if we failed to parse it.
        """

        # Parse the thing
        if 'flow' in data:
            name = "flow"
        elif '<flow>' in data:
            name = "<flow>"
        else:
            return None

        # Parse any outgoing

        # Done
        return EndNode(name, [])

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

        # Parse the thing
        if 'end' in data:
            name = "end"
        elif '<end>' in data:
            name = "<end>"
        else:
            return None

        # Done
        return EndNode(name, [])

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
