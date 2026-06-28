"""PARITY: internal/util/schema.go — the JSON-schema validator loaded from a --schema-validator directory.

The directory holds a config.json mapping each table to {schema: <file>, path: <go-template>} plus the referenced
JSON-schema files (which $ref each other via file:// URLs). new_schema_validator compiles a per-table validator;
validate(event) returns (found, valid, path) and raises SchemaValidationError when a schema is found but the event
fails it — exactly the contract batch_processor + the importer already consume.

DEVIATIONS: schema-jsonschema-lib (santhosh-tekuri/jsonschema/v5 → the Python `jsonschema` + `referencing`
registry, registered under the same file://rel / file:///rel / file://abs URIs Go uses so $refs resolve);
schema-path-gotemplate (Go html/template → a minimal {{.field}} / {{.nested.field}} substitution; no HTML escaping,
which is a no-op for the path-safe values used).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import jsonschema
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT7

from eds.dbchange import DBChangeEvent
from eds.schema import SchemaValidationError

_TMPL_RE = re.compile(r"\{\{\s*\.([a-zA-Z0-9_.]+)\s*\}\}")


def _render_path(template: str, obj: dict) -> str:
    """PARITY: rule.template.Execute — minimal Go-template {{.field}} / {{.nested.field}} substitution."""

    def repl(m: re.Match) -> str:
        value: object = obj
        for part in m.group(1).split("."):
            value = value.get(part) if isinstance(value, dict) else None
            if value is None:
                break
        return "" if value is None else str(value)

    return _TMPL_RE.sub(repl, template)


def _to_schema_event(event: DBChangeEvent) -> dict:
    """PARITY: toSchemaDBChangeEvent → JSONStringify → map — the object the schema validates against.

    Mirrors the struct's omitempty: companyId/locationId/userId/before/after/diff are dropped when nil; before/after
    are parsed objects (a JSON null parses to nil and is therefore omitted, matching Go's Unmarshal-into-map)."""
    obj: dict = {
        "operation": event.operation,
        "id": event.id,
        "table": event.table,
        "key": event.key,  # []string, no omitempty: nil -> null
        "modelVersion": event.model_version,
        "timestamp": event.timestamp,
        "mvccTimestamp": event.mvcc_timestamp,
    }
    if event.company_id is not None:
        obj["companyId"] = event.company_id
    if event.location_id is not None:
        obj["locationId"] = event.location_id
    if event.user_id is not None:
        obj["userId"] = event.user_id
    if event.before is not None:
        parsed = json.loads(event.before.value)
        if parsed is not None:
            obj["before"] = parsed
    if event.after is not None:
        parsed = json.loads(event.after.value)
        if parsed is not None:
            obj["after"] = parsed
    if event.diff:
        obj["diff"] = event.diff
    return obj


class SchemaValidator:
    """PARITY: util.SchemaValidator — per-table compiled JSON-schema + optional path template."""

    def __init__(self, rules: dict[str, tuple[jsonschema.protocols.Validator, str]]) -> None:
        self._rules = rules

    def validate(self, event: DBChangeEvent) -> tuple[bool, bool, str]:
        """PARITY: SchemaValidator.Validate — (found, valid, path); raises SchemaValidationError on a schema miss."""
        rule = self._rules.get(event.table)
        if rule is None:
            return False, False, ""  # PARITY: no schema for the table
        validator, template = rule
        obj = _to_schema_event(event)
        try:
            validator.validate(obj)
        except jsonschema.ValidationError as e:  # PARITY: ErrSchemaValidation → caller skips (debug-logs)
            raise SchemaValidationError(str(e)) from e
        path = _render_path(template, obj) if template else ""
        return True, True, path


def new_schema_validator(schema_dir: str) -> SchemaValidator:
    """PARITY: NewSchemaValidator — load config.json + the referenced schema files into a per-table validator."""
    abs_dir = os.path.abspath(schema_dir)
    config = os.path.join(abs_dir, "config.json")
    if not os.path.exists(config):
        raise RuntimeError(f"config.json not found in schema directory: {abs_dir}")
    with open(config, encoding="utf-8") as f:
        rules_cfg: dict[str, dict] = json.load(f)

    # Register every schema file (except config.json) under the file://rel / file:///rel / file://abs URIs Go uses,
    # so the files' cross $refs (e.g. "file:///models/labor.json") resolve via the registry.
    resources: list[tuple[str, Resource]] = []
    for filename in _list_files(abs_dir):
        rel = os.path.relpath(filename, abs_dir).replace(os.sep, "/")
        if rel == "config.json":
            continue
        with open(filename, encoding="utf-8") as f:
            contents = json.load(f)
        resource = Resource.from_contents(contents, default_specification=DRAFT7)
        abs_uri = "file://" + filename.replace(os.sep, "/")
        for uri in (f"file://{rel}", f"file:///{rel}", abs_uri):
            resources.append((uri, resource))
    registry = Registry().with_resources(resources)

    rules: dict[str, tuple[jsonschema.protocols.Validator, str]] = {}
    for table, rule in rules_cfg.items():
        schema_path = os.path.join(abs_dir, rule["schema"])
        if not os.path.exists(schema_path):
            raise RuntimeError(f"schema file not found: {schema_path} for table: {table}")
        with open(schema_path, encoding="utf-8") as f:
            root = json.load(f)
        validator_cls = jsonschema.validators.validator_for(root, default=jsonschema.Draft7Validator)
        # PARITY: Go's compiler.Compile is EAGER — validate the schema structure + resolve every $ref at LOAD time
        # so a misconfigured dir fails fast here (otherwise the consumer would silently skip+ack every event for
        # the table — a data-loss risk — instead of refusing to start).
        try:
            validator_cls.check_schema(root)
            _resolve_all_refs(root, registry)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"error compiling schema: {schema_path} for table: {table}: {e}") from e
        validator = validator_cls(root, registry=registry)
        rules[table] = (validator, rule.get("path", ""))
    return SchemaValidator(rules)


def _resolve_all_refs(root: dict, registry: Registry) -> None:
    """PARITY: the eager $ref resolution Go's compiler.Compile does — raise on any unresolvable ref."""
    resolver = registry.resolver()
    seen: set[str] = set()

    def walk(node: object, res: Any) -> None:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref not in seen:
                seen.add(ref)
                resolved = res.lookup(ref)  # raises if the ref cannot be resolved
                walk(resolved.contents, resolved.resolver)
            for key, value in node.items():
                if key != "$ref":
                    walk(value, res)
        elif isinstance(node, list):
            for item in node:
                walk(item, res)

    walk(root, resolver)


def _list_files(root: str) -> list[str]:
    """PARITY: util.ListDir — recursive file listing (the schema dir has base/ and models/ subdirs)."""
    out: list[str] = []
    for dirpath, _dirs, files in os.walk(root):
        out.extend(os.path.join(dirpath, name) for name in files if name != ".DS_Store")  # PARITY: ListDir skips it
    return out
