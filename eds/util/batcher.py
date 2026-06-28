"""PARITY: internal/util/batcher.go — accumulate change events into Records."""

from __future__ import annotations

from dataclasses import dataclass, field

from eds.dbchange import DBChangeEvent
from eds.util import gojson
from eds.util.gostruct import gojson_struct


@dataclass
class Record:
    """PARITY: batcher.go Record. JSON is declaration field order (table,id,operation,diff,object);
    ``event`` is json:"-" (excluded). ``object`` is a map → marshaled with sorted keys."""

    table: str = field(default="", metadata={"json": "table"})
    id: str = field(default="", metadata={"json": "id"})
    operation: str = field(default="", metadata={"json": "operation"})
    diff: list[str] | None = field(default=None, metadata={"json": "diff"})  # NEVER (null when None)
    object: dict | None = field(default=None, metadata={"json": "object"})  # NEVER (null); dict → sorted keys
    # event is json:"-" (no metadata → SKIP).
    event: DBChangeEvent | None = field(default=None, repr=False, compare=False)

    def __str__(self) -> str:
        """PARITY: Record.String — JSONStringify(r)."""
        return gojson.stringify(self)

    def __gojson__(self) -> str:
        return gojson_struct(self)


class Batcher:
    """PARITY: batcher.go Batcher."""

    def __init__(self) -> None:
        self._records: list[Record] = []

    def add(self, event: DBChangeEvent) -> None:
        """PARITY: Add — append a Record built from the event (object error is handled in the consumer)."""
        obj = event.get_object()  # PARITY: error ignored here, handled by the consumer
        self._records.append(
            Record(
                table=event.table,
                id=event.get_primary_key(),
                operation=event.operation,
                diff=event.diff,
                object=obj,
                event=event,
            )
        )

    def records(self) -> list[Record]:
        """PARITY: Records."""
        return self._records

    def clear(self) -> None:
        """PARITY: Clear."""
        self._records = []

    def __len__(self) -> int:
        """PARITY: Len."""
        return len(self._records)
