# LOG.py
#   by Lut99
#
# Created:
#   10 Jul 2024, 20:29:35
# Last edited:
#   10 Jul 2024, 22:19:55
# Auto updated?
#   Yes
#
# Description:
#   Defines some logging functions.
#

import os
import sys
from io import TextIOWrapper
from typing import Optional


##### HELPER FUNCTIONS #####
def _supports_color():
    """
        Returns True if the running system's terminal supports color, and False
        otherwise.

        From: https://stackoverflow.com/a/22254892
    """
    plat = sys.platform
    supported_platform = plat != 'Pocket PC' and (plat != 'win32' or
                                                  'ANSICON' in os.environ)
    # isatty is not always implemented, #6223.
    is_a_tty = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    return supported_platform and is_a_tty





##### LIBRARY #####
def pwarn(text: str, end: str = '\n', use_colour: Optional[bool] = None, file: TextIOWrapper = sys.stderr):
    """
        Prints a message as if it's a warning.

        # Arguments
        - `text`: The message to display.
        - `end`: Something to print at the end of the message. By default, this is a newline.
        - `use_colour`: Whether to use colour or not. Use `None` to try and deduce it automagically.
    """

    # Resolve colours
    use_colour = use_colour if use_colour is not None else _supports_color()
    accent = "\033[93;1m" if use_colour else ""
    bold = "\033[1m" if use_colour else ""
    clear = "\033[0m" if use_colour else ""

    # Print the message
    print(f"{accent}WARNING{clear}{bold}: {text}{clear}", file=file, end=end)

def perror(text: str, end: str = '\n', use_colour: Optional[bool] = None, file: TextIOWrapper = sys.stderr):
    """
        Prints a message as if it's a fatal error.

        # Arguments
        - `text`: The message to display.
        - `end`: Something to print at the end of the message. By default, this is a newline.
        - `use_colour`: Whether to use colour or not. Use `None` to try and deduce it automagically.
    """

    # Resolve colours
    use_colour = use_colour if use_colour is not None else _supports_color()
    accent = "\033[91;1m" if use_colour else ""
    bold = "\033[1m" if use_colour else ""
    clear = "\033[0m" if use_colour else ""

    # Print the message
    print(f"{accent}ERROR{clear}{bold}: {text}{clear}", file=file, end=end)
