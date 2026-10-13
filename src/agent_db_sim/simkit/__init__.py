"""simkit: project-local scaffolding layer over SimPy (the kernel).

Contents: component base class + lifecycle driving, typed ports with an
introspectable connect graph, ingress/processing/bandwidth primitives,
config->class registries, the factorial sweep runner and the probe API with
an in-memory sink.
"""

from agent_db_sim.simkit.component import Component, Composite
from agent_db_sim.simkit.errors import (
    BandwidthError,
    ComponentError,
    IngressError,
    RegistryError,
    SimkitError,
    SweepError,
    WiringError,
)
from agent_db_sim.simkit.ingress import open_ingress
from agent_db_sim.simkit.levels import BandwidthPool
from agent_db_sim.simkit.ports import ConnectGraph, Link
from agent_db_sim.simkit.processing import ServiceRecord, SlotServer
from agent_db_sim.simkit.probes import InMemorySink, NullSink, ProbeSink, Sample
from agent_db_sim.simkit.registry import build_model
from agent_db_sim.simkit.runner import ModelResult, run_model
from agent_db_sim.simkit.sweep import RunResult, run_sweep

__all__ = [
    "Component",
    "Composite",
    "ConnectGraph",
    "Link",
    "IngressError",
    "ComponentError",
    "RegistryError",
    "SimkitError",
    "SweepError",
    "WiringError",
    "BandwidthError",
    "SlotServer",
    "ServiceRecord",
    "BandwidthPool",
    "open_ingress",
    "build_model",
    "run_model",
    "ModelResult",
    "run_sweep",
    "RunResult",
    "ProbeSink",
    "InMemorySink",
    "NullSink",
    "Sample",
]
