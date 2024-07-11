#!/usr/bin/env python3
# WORKFLOW AGENT.py
#   by Lut99
#
# Created:
#   10 Jul 2024, 19:52:51
# Last edited:
#   11 Jul 2024, 19:17:46
# Auto updated?
#   Yes
#
# Description:
#   Implements the entrypoint for the SENSE Workflow Agent script.
#   
#   Certain core parts of the agent can be found in `lib/`.
#

import argparse
import sys
import time

import lib.log as log
import lib.workflow as workflow


def main(wf_path: str) -> int:
    """
        Entrypoint to the script.

        # Arguments
        - `wf_path`: Path to the workflow file this agent should run.

        # Returns
        The exit code of the script. `0` means all was OK.
    """

    # Open a file handle
    if wf_path == '-':
        handle = sys.stdin
    else:
        try:
            handle = open(wf_path, "r")
        except IOError as e:
            log.perror(f"Failed to open workflow file '{wf_path}': {e}")
            return e.errno

    # Parse the workflow
    wf = workflow.Workflow.parse_handle(wf_path if wf_path != '-' else "<stdin>", handle, "yaml")
    log.pdebug(wf.fmt(), end="")

    # Traverse it
    log.pinfo(f"Running workflow '{wf.name}'...")
    stack = [wf.graph]
    while len(stack) > 0:
        node = stack.pop()
        log.pdebug(f"Processing node '{wf.name}:{node.name}'")

        # Wait until all incoming flows have been finished
        waiting = True
        notified = set()
        while waiting:
            waiting = False
            for prev in node.incoming:
                if isinstance(prev, workflow.FlowNode):
                    if not prev.flow.done:
                        if prev.name not in notified:
                            log.pdebug(f"Waiting for incoming node '{wf.name}:{node.name}' to complete...")
                            notified.add(prev.name)
                        waiting = True
            if waiting:
                time.sleep(0.5)

        # Match on the type
        if isinstance(node, workflow.FlowNode):
            # Start the flow
            node.flow.spawn()

        # Continue by scheduling all outgoing ones
        for next in node.outgoing:
            stack.append(next)

    # Done
    return 0


# Real entrypoint
if __name__ == "__main__":
    # Define the CLI arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("WORKFLOW", type=str, help="The workflow file that this agent should run. You can use '-' to refer to stdin.")
    parser.add_argument("-v", "--debug", action="store_true", help="If given, provides additional DEBUG-level log statements.")

    # Parse the arguments
    args = parser.parse_args()
    log.DEBUG = args.debug

    # Run main
    exit(main(args.WORKFLOW))
