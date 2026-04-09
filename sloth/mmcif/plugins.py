"""
SLOTH Plugin System

Generic, instance-level plugin registry that extends dot-notation access
on any level of the data hierarchy (Category, DataBlock, MMCIFDataContainer).

Plugins are accessed as attributes::

    block._atom_site.validate()          # validation plugin
    block._atom_site.statistics()        # custom stats plugin
    block._atom_site.statistics().result # access computed value
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Category, DataBlock, MMCIFDataContainer


# ---------------------------------------------------------------------------
# Base classes
# ---------------------------------------------------------------------------

class PluginWrapper:
    """Chainable wrapper returned when a plugin is accessed via dot-notation.

    Calling the wrapper executes the plugin and returns ``self`` so that
    additional methods (defined by subclasses) can be chained::

        block._atom_site.validate().against(block._entity)
        value = block._atom_site.statistics().result
    """

    def __init__(self, target, plugin: "Plugin"):
        self._target = target
        self._plugin = plugin
        self._result = None

    def __call__(self, *args, **kwargs) -> "PluginWrapper":
        """Execute the plugin on the bound target. Returns *self* for chaining."""
        self._result = self._plugin.execute(self._target, *args, **kwargs)
        return self

    @property
    def result(self) -> Any:
        """The return value of the last :meth:`__call__` invocation."""
        return self._result


class Plugin(ABC):
    """Abstract base class for plugins that extend dot-notation functionality."""

    @abstractmethod
    def create_wrapper(self, target) -> PluginWrapper:
        """Return a :class:`PluginWrapper` (or subclass) bound to *target*."""
        pass

    @abstractmethod
    def execute(self, target, *args, **kwargs) -> Any:
        """Run the plugin logic on *target*. Called by :meth:`PluginWrapper.__call__`."""
        pass


class FunctionPlugin(Plugin):
    """Adapter that wraps a plain callable as a :class:`Plugin`."""

    def __init__(self, func: Callable):
        self._func = func

    def create_wrapper(self, target) -> PluginWrapper:
        return PluginWrapper(target, self)

    def execute(self, target, *args, **kwargs) -> Any:
        return self._func(target, *args, **kwargs)


# ---------------------------------------------------------------------------
# Plugin factory
# ---------------------------------------------------------------------------

class PluginFactory:
    """Lightweight plugin registry keyed by name.

    Plugins are registered with a *name* (the attribute that will appear
    via dot-notation access) and can be looked up or listed.
    """

    def __init__(self):
        self._plugins: Dict[str, Plugin] = {}

    # -- registration -------------------------------------------------------

    def register(self, name: str, plugin) -> None:
        """Register a plugin.

        :param name: The dot-notation attribute name (e.g. ``"validate"``).
        :param plugin: A :class:`Plugin` instance **or** a plain callable
            (auto-wrapped as :class:`FunctionPlugin`).
        """
        if not isinstance(plugin, Plugin):
            if callable(plugin):
                plugin = FunctionPlugin(plugin)
            else:
                raise TypeError(
                    f"Plugin must be a Plugin instance or callable, got {type(plugin)}"
                )
        self._plugins[name] = plugin

    # -- lookup -------------------------------------------------------------

    def get_wrapper(self, name: str, target) -> Optional[PluginWrapper]:
        """Return a bound :class:`PluginWrapper` for *name*, or ``None``."""
        plugin = self._plugins.get(name)
        if plugin is None:
            return None
        return plugin.create_wrapper(target)

    def has_plugin(self, name: str) -> bool:
        """Return ``True`` if a plugin is registered for *name*."""
        return name in self._plugins

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Return the raw :class:`Plugin` for *name*, or ``None``."""
        return self._plugins.get(name)

    def list_plugins(self) -> List[str]:
        """Return registered plugin names."""
        return list(self._plugins.keys())
