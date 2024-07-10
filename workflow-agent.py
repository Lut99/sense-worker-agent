#!/usr/bin/env python3
# WORKFLOW AGENT.py
#   by Lut99
#
# Created:
#   10 Jul 2024, 19:52:51
# Last edited:
#   10 Jul 2024, 22:15:58
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
    print(wf.fmt())

    return 0


# Real entrypoint
if __name__ == "__main__":
    # Define the CLI arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("WORKFLOW", type=str, help="The workflow file that this agent should run. You can use '-' to refer to stdin.")

    # Parse the arguments
    args = parser.parse_args()

    # Run main
    exit(main(args.WORKFLOW))
