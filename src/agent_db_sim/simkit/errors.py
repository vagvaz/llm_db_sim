"""Simkit exception hierarchy.

All simkit kernel errors fail fast: they are raised during construction,
elaboration or sweep enumeration — before any simulated time has passed.
"""

from __future__ import annotations


class SimkitError(Exception):
    """Base class for all simkit kernel errors."""


class ComponentError(SimkitError):
    """Raised when lifecycle methods or primitives are misused."""


class WiringError(SimkitError):
    """Raised at elaboration when declared ports/links are inconsistent."""


class RegistryError(SimkitError):
    """Raised when a config name does not map to a registered class."""


class IngressError(SimkitError):
    """Raised for bad ingress specifications or request key lookups."""


class BandwidthError(SimkitError):
    """Raised for invalid bandwidth/level primitive usage."""


class SweepError(SimkitError):
    """Raised for invalid sweep configuration or run-directory layout."""
