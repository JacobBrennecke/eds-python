"""PARITY: internal/util/optimize.go — RecordOptimize (sort by mvcc, combine same-PK records).

Used by the Snowflake driver's flush to collapse a batch before generating MERGE/DELETE statements.
"""

from __future__ import annotations

from eds.util.batcher import Record


def sort_records_by_mvcc_timestamp(records: list[Record]) -> list[Record]:
    """PARITY: SortRecordsByMVCCTimestamp — ascending by float(event.mvcc_timestamp); missing/unparsable → 0.
    Stable (Python sort), mutates in place, returns the list."""

    def key(r: Record) -> float:
        if r.event is None:
            return 0.0
        try:
            return float(r.event.mvcc_timestamp)
        except (ValueError, TypeError):
            return 0.0

    records.sort(key=key)
    return records


def combine_records_with_same_primary_key(records: list[Record]) -> list[Record]:
    """PARITY: CombineRecordsWithSamePrimaryKey — group by table+id (first-seen order), collapse each group:
    consecutive UPDATEs merge (diff unioned, objects merged, latest event kept); a DELETE resets the group to
    just that DELETE; INSERT-then-UPDATE stays two records."""
    groups: dict[str, list[Record]] = {}
    order: list[str] = []
    for r in records:
        k = r.table + r.id
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(r)
    result: list[Record] = []
    for k in order:
        result.extend(_combine(groups[k]))
    return result


def _combine(records: list[Record]) -> list[Record]:
    if len(records) == 1:
        return [records[0]]
    collapsed = [records[0]]
    previous = records[0]
    for record in records[1:]:
        bt = _batch_type(record, previous)
        if bt == "delete_with_batch":
            collapsed = [record]
            previous = record
        elif bt == "update_with_batch":
            pdiff = previous.diff if previous.diff is not None else []
            for d in record.diff or []:
                if d not in pdiff:
                    pdiff.append(d)
            previous.diff = pdiff
            previous.event = record.event
            if previous.object is None:
                previous.object = {}
            previous.object.update(record.object or {})
        else:  # without_batch
            collapsed.append(record)
            previous = record
    return collapsed


def _batch_type(record: Record, previous: Record) -> str:
    if record.operation == "DELETE":
        return "delete_with_batch"
    if record.operation == "UPDATE":
        if previous.operation == "UPDATE":
            return "update_with_batch"
        return "without_batch"  # previous INSERT (or DELETE)
    return "without_batch"  # INSERT
