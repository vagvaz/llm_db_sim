"""Typed ports and the introspectable connect graph.

Every component declares named inputs/outputs as class attributes mapping
port name -> port kind (an arbitrary comparable tag, typically a string).
``connect()`` records links; the full check — port existence, kind equality,
no double-wired inputs — runs at elaboration (fail fast, before simulated
time). The recorded link list is the introspection surface (future DOT
rendering falls out for free).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Set, Tuple

from agent_db_sim.simkit.errors import WiringError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from agent_db_sim.simkit.component import Component

__all__ = ["Link", "ConnectGraph"]


@dataclass(frozen=True)
class Link:
    """One declared connection ``src.output_port -> dst.input_port``."""

    src: "Component"
    src_port: str
    dst: "Component"
    dst_port: str

    def describe(self) -> str:
        """Human-readable edge description for error messages."""
        return (
            f"{self.src.full_path}.{self.src_port} -> "
            f"{self.dst.full_path}.{self.dst_port}"
        )


@dataclass
class ConnectGraph:
    """The recorded connect graph of one composite."""

    links: List[Link] = field(default_factory=list)

    def connect(
        self, src: "Component", src_port: str, dst: "Component", dst_port: str
    ) -> Link:
        """Record a link. Full type-checking happens at ``validate()``."""
        link = Link(src=src, src_port=src_port, dst=dst, dst_port=dst_port)
        self.links.append(link)
        return link

    def validate(self) -> None:
        """Fail fast: unknown ports, kind mismatches, double-wired inputs."""
        wired: Set[Tuple[int, str]] = set()
        for link in self.links:
            self._check_endpoints(link)
            key = (id(link.dst), link.dst_port)
            if key in wired:
                raise WiringError(
                    f"input port {link.dst.full_path}.{link.dst_port} "
                    f"is wired more than once"
                )
            wired.add(key)

    def links_of(self, component: "Component") -> List[Link]:
        """All links touching ``component`` (introspection surface)."""
        return [
            link for link in self.links if link.src is component or link.dst is component
        ]

    def __len__(self) -> int:
        return len(self.links)

    def _check_endpoints(self, link: Link) -> None:
        if link.src_port not in link.src.output_ports:
            raise WiringError(
                f"{link.src.full_path} declares no output port {link.src_port!r} "
                f"(declared: {sorted(link.src.output_ports)})"
            )
        if link.dst_port not in link.dst.input_ports:
            raise WiringError(
                f"{link.dst.full_path} declares no input port {link.dst_port!r} "
                f"(declared: {sorted(link.dst.input_ports)})"
            )
        kind_out = link.src.output_ports[link.src_port]
        kind_in = link.dst.input_ports[link.dst_port]
        if kind_out != kind_in:
            raise WiringError(
                f"port kind mismatch on {link.describe()}: "
                f"producer kind {kind_out!r} != consumer kind {kind_in!r}"
            )
