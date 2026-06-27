"""PARITY: internal/importer/importer.go — the import Handler protocol.

The ImportRunner replay engine (importer.go Run) is ported at M5; this module currently defines the Handler
interface that the drivers implement (and the runner drives) so M4 drivers can declare it.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from eds.dbchange import DBChangeEvent
from eds.schema import Schema, SchemaMap


@runtime_checkable
class Handler(Protocol):
    """PARITY: importer.Handler (the import-handler interface)."""

    def create_datasource(self, schema: SchemaMap) -> None: ...
    def import_event(self, event: DBChangeEvent, schema: Schema) -> None: ...
    def import_completed(self) -> None: ...
