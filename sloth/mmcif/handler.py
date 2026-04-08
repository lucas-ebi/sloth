from typing import Callable, Optional, List, Tuple, Union
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter
from .importer import JSONImporter
from .models import MMCIFDataContainer, DataBlock, Category, DataSourceFormat
from .plugins import PluginFactory, Plugin
from .defaults import PluginScope
from .validator import (
    MmcifValidator,
    ValidationError, ValidationReport, ValidatorPlugin,
    BlockValidator,
)


class MMCIFHandler:
    """A class to handle reading and writing mmCIF files with high-performance gemmi backend."""

    def __init__(self):
        """Initialize the handler with gemmi backend for optimal performance."""
        self._plugin_factory = PluginFactory()
        self._parser = None
        self._writer = None
        self._file_obj = None

    @property
    def plugin_factory(self) -> PluginFactory:
        """The underlying plugin factory (read-only, for advanced use)."""
        return self._plugin_factory

    def register(
        self,
        name: str,
        plugin,
        *,
        scope: PluginScope,
    ) -> None:
        """Register a plugin for dot-notation access.

        :param name: The attribute name (e.g. ``"validate"``, ``"statistics"``).
        :param plugin: A :class:`Plugin` instance or a plain callable.
        :param scope: A :class:`PluginScope` member.

        Example::

            from sloth.mmcif import PluginScope
            from sloth.mmcif.validator import ValidatorPlugin
            handler.register("validate", ValidatorPlugin(), scope=PluginScope.CATEGORY)

            # Or a simple function plugin
            handler.register("stats", lambda cat: cat.row_count, scope=PluginScope.CATEGORY)
        """
        self._plugin_factory.register(name, plugin, scope=scope)

    def read(
        self, 
        filename: str, 
        categories: Optional[List[str]] = None
    ) -> MMCIFDataContainer:
        """
        Parse an mmCIF file and returns a data container using gemmi's high-performance backend.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse. If None, all categories are included.
        :type categories: Optional[List[str]]
        :return: The data container with lazy-loaded items.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(
            plugin_factory=self._plugin_factory,
            categories=categories,
        )
        container = self._parser.parse(filename)
        
        return container

    def write(
        self, 
        mmcif: MMCIFDataContainer, 
        filename: Optional[str] = None
    ) -> None:
        """
        Writes a data container to a file using gemmi's high-performance backend.

        :param mmcif: The data container to write.
        :type mmcif: MMCIFDataContainer
        :param filename: Optional filename to write to. If not provided, uses pre-set file object.
        :type filename: Optional[str]
        :return: None
        """
        self._writer = MMCIFWriter()
        
        if filename:
            # Write to specified filename
            with open(filename, 'w') as file_obj:
                self._writer.write(file_obj, mmcif)
        elif hasattr(self, "_file_obj") and self._file_obj:
            # Write to pre-set file object
            self._writer.write(self._file_obj, mmcif)
        else:
            raise IOError("No filename provided and file is not open for writing")

    def export(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format.

        :param mmcif: The data container to export
        :type mmcif: MMCIFDataContainer
        :param file_path: Path to save the file (optional)
        :type file_path: Optional[str]
        :param kwargs: Additional options (e.g., indent, quiet)
        :return: String representation if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        return self._export_json(mmcif, file_path, **kwargs)

    def load(
        self,
        file_path: str,
        **kwargs
    ) -> MMCIFDataContainer:
        """
        Import mmCIF data from JSON format.

        :param file_path: Path to the JSON file to import
        :type file_path: str
        :param kwargs: Additional options
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        return self._import_json(file_path, **kwargs)

    # Private methods for specific format handling
    def _export_json(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        **kwargs
    ) -> Optional[str]:
        """Export to JSON format (always nested)."""
        denormalize = kwargs.get('denormalize', False)
        exporter = JSONExporter(quiet=kwargs.get('quiet', False), denormalize=denormalize)
        indent = kwargs.get('indent', None)
        return exporter.export_data(mmcif, file_path, indent)

    def _import_json(
        self,
        file_path: str,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from JSON format (assumes nested structure)."""
        importer = JSONImporter()
        container = importer.import_data(file_path)
        container.source_format = DataSourceFormat.JSON
        return container

    # -- validation ---------------------------------------------------------

    def validate(
        self,
        data: Union[MMCIFDataContainer, DataBlock, Category],
        *,
        relaxed: bool = False,
    ) -> "ValidationReport":
        """Validate mmCIF data recursively and return a :class:`ValidationReport`.

        Works on any level of the hierarchy — a single
        :class:`~sloth.mmcif.models.Category`, a
        :class:`~sloth.mmcif.models.DataBlock`, or an entire
        :class:`~sloth.mmcif.models.MMCIFDataContainer`.

        The rules that run depend on the *relaxed* flag:

        * **default** (``relaxed=False``): the full
          :class:`~sloth.mmcif.validator.MmcifValidator` dictionary + wwPDB suite
          runs first, followed by any user-registered custom validators.
        * **relaxed** (``relaxed=True``): *only* user-registered validators
          run.  If none have been registered the report will be empty.

        :param data: The data object to validate.
        :param relaxed: When ``True``, skip the official MmcifValidator and
            only run user-registered rules.
        :return: A :class:`ValidationReport` with all collected issues.

        Example::

            handler = MMCIFHandler()
            report = handler.validate(container)              # full suite
            report = handler.validate(container, relaxed=True) # user rules only
        """
        # Reject bad types early
        if not isinstance(data, (MMCIFDataContainer, DataBlock, Category)):
            raise TypeError(
                f"Expected Category, DataBlock, or MMCIFDataContainer, "
                f"got {type(data).__name__}"
            )

        # Build the effective validator based on relaxed flag
        user_plugin = self._plugin_factory.get_plugin(
            "validate", PluginScope.CATEGORY
        )

        if not relaxed:
            # Full validation: official MmcifValidator + user rules
            effective: ValidatorPlugin = MmcifValidator()
            # Merge any user-registered custom rules on top
            if (
                user_plugin is not None
                and isinstance(user_plugin, ValidatorPlugin)
                and not isinstance(user_plugin, MmcifValidator)
            ):
                effective = effective.merge(user_plugin)
        else:
            # Relaxed: only user-registered validators
            if user_plugin is not None and isinstance(user_plugin, ValidatorPlugin):
                effective = user_plugin
            else:
                # Nothing registered — return an empty report
                return ValidationReport()

        if isinstance(data, MMCIFDataContainer):
            bv = BlockValidator(effective)
            report = ValidationReport()
            for block_name in data.blocks:
                report.extend(bv.execute(data[block_name]))
            return report

        if isinstance(data, DataBlock):
            return BlockValidator(effective).execute(data)

        if isinstance(data, Category):
            report = ValidationReport()
            for validator_fn in effective.get_validators(data.name):
                try:
                    validator_fn(data)
                except ValidationError as exc:
                    report.add(exc)
            return report
