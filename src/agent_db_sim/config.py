"""Scenario config loading helpers (OmegaConf + named profile overlays).

Kept deliberately small: ticket 03 will formalize the config schema. The
convention here is that a scenario YAML may carry a top-level ``profiles:``
key mapping named overlays; alternatively a profile can live in a sibling
file named ``<profile>.yaml`` next to the scenario.
"""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig, OmegaConf


class ConfigError(Exception):
    """Raised for missing/invalid scenario or profile configuration."""


def resolve_profile(
    profile: str,
    profiles: DictConfig,
    scenario_dir: Path,
) -> DictConfig:
    """Resolve a named profile to an overlay DictConfig.

    Lookup order: a key under the scenario's ``profiles:`` section first,
    then a sibling file ``<profile>.yaml`` in the scenario's directory.
    """
    if profile in profiles:
        return OmegaConf.create(profiles[profile])

    sibling = scenario_dir / f"{profile}.yaml"
    if sibling.is_file():
        return OmegaConf.load(sibling)

    names = sorted(profiles.keys()) if len(profiles) else ["<none>"]
    raise ConfigError(
        f"unknown profile {profile!r}: not a key under 'profiles:' of the "
        f"scenario and no sibling file {sibling.name!r}. "
        f"Available profiles: {', '.join(names)}"
    )


def load_scenario(scenario_path: str | Path, profile: str | None = None) -> DictConfig:
    """Load a scenario YAML, optionally applying a named profile overlay.

    The profile overlay is deep-merged on top of the base scenario config.
    """
    path = Path(scenario_path)
    if not path.is_file():
        raise ConfigError(f"scenario file not found: {path}")

    cfg = OmegaConf.load(path)

    profiles: DictConfig = OmegaConf.create({})
    if "profiles" in cfg:
        profiles = cfg.profiles
        del cfg["profiles"]

    if profile is not None:
        overlay = resolve_profile(profile, profiles, path.parent)
        cfg = OmegaConf.merge(cfg, overlay)

    return cfg
