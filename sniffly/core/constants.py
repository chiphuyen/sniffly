"""
Constants used throughout the Claude Analytics application.

This module contains constants that may need to be updated if Claude's
log format changes in future versions.
"""

# User interruption message patterns
# These are the messages that appear when a user interrupts Claude during tool use
# Update these if Claude changes the format in future versions
USER_INTERRUPTION_PREFIX = "[Request interrupted by user for tool use]"
USER_INTERRUPTION_API_ERROR = "API Error: Request was aborted."

# List of all interruption patterns for easier checking
USER_INTERRUPTION_PATTERNS = [USER_INTERRUPTION_PREFIX, USER_INTERRUPTION_API_ERROR]

# Note: The USER_INTERRUPTION_PREFIX constant is also defined in dashboard.html JavaScript
# If you update this, also update the USER_INTERRUPTION_PREFIX constant in:
# - sniffly/shared/templates/dashboard.html

"""
Central definitions of all error-detection patterns.

Each value is a list of **regular-expression** patterns that, when
`re.search` finds a match (case-insensitive), map the error message
to the key's category.
"""

ERROR_PATTERNS: dict[str, list[str]] = {
    # ─────────────────── user-driven interruptions ───────────────────
    "User Interruption": [
        r"user doesn't want to proceed",
        r"user doesn't want to take this action",
        r"\[Request interrupted",
    ],
    # ─────────────────── timed out ───────────────────
    "Command Timeout": [r"Command timed out"],
    # ────────────────────────── file state ───────────────────────────
    "File Not Read": [r"File has not been read yet"],
    "File Modified": [r"File has been modified since read"],
    "File Too Large": [r"exceeds maximum allowed"],
    # ───────────────────────── missing content ───────────────────────
    "Content Not Found": [
        r"String to replace not found",
        r"String not found in file",
        r"No module named",
        r"No such file or directory",
        r"File does not exist",
        r"npm error enoent Could not read package\.json",
    ],
    # ────────────────────────── no-op cases ──────────────────────────
    "No Changes": [r"No changes to make"],
    # ────────────────────────── permissions ──────────────────────────
    "Permission Error": [
        r"Permission denied",
        # both parts must appear → use a single regex with lookahead
        r"(?=.*cd to)(?=.*was blocked)",
    ],
    # ───────────────────── runtime / environment ─────────────────────
    "Tool Not Found": [r"command not found"],
    "Wrong Tool": [r"File is a Jupyter Notebook. Use the NotebookRead to read this file."],
    "Code Runtime Error": [
        r"Cannot find module",
        r"Traceback",
        r"asyncio_default_fixture_loop_scope",
    ],
    "Port Binding Error": [r"while attempting to bind on address"],
    # ───────────────────────── syntax / parse ────────────────────────
    "Syntax Error": [
        r"SyntaxError",
        r"syntax error",
        r"matches of the string to replace, but replace_all is false",
        r"null \(null\) has no keys",
        r"kill: %1: no such job",
        r"jq: error",
        r"InputValidationError",
    ],
    # ───────────────────────── notebook cells not found ───────────────────────────
    "Notebook Cell Not Found": [
        r'Cell with ID "[a-z0-9]+" not found in notebook'
    ],
    # ───────────────────────── other tools ───────────────────────────
    "Other Tool Errors": [r"\[Details] Error: Error"],
}
