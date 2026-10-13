"""Small fail-fast config accessors for the ``db.*`` namespace.

The DB config may arrive as an OmegaConf ``DictConfig`` or a plain mapping;
the helpers here accept both. Unknown keys fall back to defaults so
concurrent tickets can extend the schema without breaking this component.
"""

from __future__ import annotations

from typing import Any, Optional

__all__ = ["sub", "cget", "cfloat", "cint", "cstr", "cbool"]


def sub(cfg: Any, key: str, default: Optional[Any] = None) -> Any:
    """Descend into ``cfg[key]`` when present, else return ``default``."""
    if cfg is None:
        return default
    try:
        if key in cfg:
            return cfg[key]
    except Exception:
        return default
    return default


def cget(cfg: Any, key: str, default: Any) -> Any:
    """Read ``cfg[key]`` with a fallback default."""
    return sub(cfg, key, default)


def cfloat(cfg: Any, key: str, default: float) -> float:
    value = cfloat_opt(cfg, key, default)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"config key {key!r} must be a number, got {value!r}") from exc


def cfloat_opt(cfg: Any, key: str, default: Optional[float]) -> Optional[float]:
    value = cget(cfg, key, default)
    if value is None:
        return None
    return float(value)


def cint(cfg: Any, key: str, default: int) -> int:
    value = cget(cfg, key, default)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"config key {key!r} must be an integer, got {value!r}") from exc


def cstr(cfg: Any, key: str, default: str) -> str:
    value = cget(cfg, key, default)
    if value is None:
        return default
    return str(value)


def cbool(cfg: Any, key: str, default: bool) -> bool:
    value = cget(cfg, key, default)
    if value is None:
        return default
    return bool(value)
