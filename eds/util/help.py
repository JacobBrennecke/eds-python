"""PARITY: internal/util/help.go — GenerateHelpSection (the colorized driver-help section builder).

Go colorizes via fatih/color (green title + white-bold body), which AUTO-DISABLES on a non-TTY (CI / piped
output), so on every non-interactive run Go emits plain ``title + "\n\n" + body``. The port emits that plain
form (matching the C# port's Help.cs decision); it is content-identical after ``ansi_strip`` — which is what
the driver-metadata configurations path applies. DEVIATION: see DEVIATIONS.md#help-generate-section.
"""

from __future__ import annotations


def generate_help_section(title: str, body: str) -> str:
    """PARITY: util.GenerateHelpSection — Go renders ``green(title) + "\n\n" + whiteBold(body)``; on a
    non-TTY fatih/color drops the SGR codes, leaving exactly this plain text."""
    return title + "\n\n" + body
