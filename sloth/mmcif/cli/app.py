"""
SLOTH TUI — Main application.

A mainframe-inspired terminal interface for browsing, editing, and building
mmCIF files with schema-aware autocomplete and a gamified UX.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen, ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Static,
)
from textual.widgets import TextArea
from textual.coordinate import Coordinate

from ..handler import MMCIFHandler
from ..models import (
    MMCIFDataContainer,
    DataBlock,
    Category,
    Item,
    DataSourceFormat,
)
from ..validator import MMCIFValidator
from .widgets import (
    SPLASH_ART,
    CIFTree,
    CompletionMeter,
    HintPanel,
    CommandInput,
)
from .schema import SchemaHints


# ═══════════════════════════════════════════════════════════════════════════
# Splash / Welcome screen
# ═══════════════════════════════════════════════════════════════════════════

class SplashScreen(Screen):
    """Full-screen welcome with arcade-style menu."""

    BINDINGS = [
        Binding("o", "open_file", "Open file"),
        Binding("n", "new_file", "New file"),
        Binding("q", "quit_app", "Quit"),
    ]

    DEFAULT_CSS = """
    SplashScreen {
        align: center middle;
        background: #0a0a0a;
    }
    #splash-art {
        width: auto;
        height: auto;
        content-align: center middle;
        text-align: center;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static(SPLASH_ART, id="splash-art")

    def action_open_file(self) -> None:
        self.app.push_screen(FileOpenScreen())

    def action_new_file(self) -> None:
        self.app.push_screen(NewBlockScreen())

    def action_quit_app(self) -> None:
        self.app.exit()


# ═══════════════════════════════════════════════════════════════════════════
# File-open modal
# ═══════════════════════════════════════════════════════════════════════════

class FileOpenScreen(ModalScreen[Optional[str]]):
    """Modal dialog to enter a file path."""

    DEFAULT_CSS = """
    FileOpenScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #file-dialog {
        width: 70;
        height: auto;
        max-height: 20;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #file-dialog Label {
        margin-bottom: 1;
    }
    """

    BINDINGS = [
        Binding("tab", "autocomplete", "Autocomplete", show=False, priority=True),
        Binding("escape", "cancel", "Cancel"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="file-dialog"):
            yield Label(
                "[bold bright_green]╔══ OPEN mmCIF FILE ══╗[/]\n"
                "[dim]Enter the path to an mmCIF (.cif) file:[/]"
            )
            yield Input(
                placeholder="/path/to/structure.cif",
                id="file-path-input",
            )
            yield Label(
                "[dim]Enter ↵ to open  │  Esc to cancel[/]",
            )

    def on_mount(self) -> None:
        self.query_one("#file-path-input", Input).focus()

    @on(Input.Submitted, "#file-path-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        path = event.value.strip()
        if not path:
            return
        expanded = os.path.expanduser(path)
        if not os.path.isfile(expanded):
            self.query_one("#file-path-input", Input).value = ""
            self.query_one("#file-path-input", Input).placeholder = (
                f"File not found: {path} — try again"
            )
            return
        self.dismiss(expanded)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# New-block modal (for Build mode)
# ═══════════════════════════════════════════════════════════════════════════

class NewBlockScreen(ModalScreen[Optional[str]]):
    """Modal to create a new mmCIF data block."""

    DEFAULT_CSS = """
    NewBlockScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #new-block-dialog {
        width: 60;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def compose(self) -> ComposeResult:
        with Vertical(id="new-block-dialog"):
            yield Label(
                "[bold bright_green]╔══ NEW mmCIF ══╗[/]\n"
                "[dim]Enter a data block name (e.g. DEMO, 1ABC):[/]"
            )
            yield Input(
                placeholder="data block name",
                id="block-name-input",
            )
            yield Label("[dim]Enter ↵ to create  │  Esc to cancel[/]")

    def on_mount(self) -> None:
        self.query_one("#block-name-input", Input).focus()

    @on(Input.Submitted, "#block-name-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        name = event.value.strip()
        if name:
            self.dismiss(name)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Add-category modal
# ═══════════════════════════════════════════════════════════════════════════

class AddCategoryScreen(ModalScreen[Optional[str]]):
    """Modal to add a new category with schema autocomplete hints."""

    DEFAULT_CSS = """
    AddCategoryScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #add-cat-dialog {
        width: 70;
        height: auto;
        max-height: 30;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #cat-suggestions {
        height: auto;
        max-height: 12;
        margin-top: 1;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(
        self,
        hints: SchemaHints,
        present_categories: Optional[Set[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._hints = hints
        self._present_categories = set(present_categories or set())

    def compose(self) -> ComposeResult:
        with Vertical(id="add-cat-dialog"):
            yield Label(
                "[bold bright_green]╔══ ADD CATEGORY ══╗[/]\n"
                "[dim]Type a category name (e.g. _atom_site).\n"
                "Suggestions appear as you type.[/]"
            )
            yield Input(
                placeholder="_category_name",
                id="cat-name-input",
            )
            yield Static(id="cat-suggestions")
            yield Label("[dim]Enter ↵ to add  │  Esc to cancel[/]")

    def on_mount(self) -> None:
        self.query_one("#cat-name-input", Input).focus()
        self._show_suggestions("")

    @on(Input.Changed, "#cat-name-input")
    def handle_change(self, event: Input.Changed) -> None:
        self._show_suggestions(event.value.strip())

    def on_key(self, event) -> None:
        if event.key == "tab":
            self.action_autocomplete()
            event.prevent_default()
            event.stop()

    def _top_category_suggestion(self, prefix: str) -> Optional[str]:
        required, matches = self._hints.suggested_categories(
            prefix,
            self._present_categories,
            limit=10,
        )
        if required:
            return required[0]
        if matches:
            return matches[0]
        return None

    def _show_suggestions(self, prefix: str) -> None:
        required, matches = self._hints.suggested_categories(
            prefix,
            self._present_categories,
            limit=10,
        )
        if required or matches:
            chunks = []
            if required:
                req_lines = "  ".join(f"[bold bright_red]{m}[/]" for m in required)
                chunks.append(f"[bold bright_yellow]⚠ MISSING REQUIRED PARENTS:[/]\n  {req_lines}")
            if matches:
                match_lines = "  ".join(f"[bright_cyan]{m}[/]" for m in matches)
                chunks.append(f"[bold bright_yellow]⚡ OTHER SUGGESTIONS:[/]\n  {match_lines}")
            self.query_one("#cat-suggestions", Static).update("\n".join(chunks))
        else:
            self.query_one("#cat-suggestions", Static).update(
                "[dim]No matching categories in dictionary[/]"
            )

    def action_autocomplete(self) -> None:
        input_widget = self.query_one("#cat-name-input", Input)
        prefix = input_widget.value.strip()
        suggestion = self._top_category_suggestion(prefix)
        if not suggestion:
            return
        input_widget.value = suggestion
        self._show_suggestions(suggestion)

    @on(Input.Submitted, "#cat-name-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        name = event.value.strip()
        if name:
            if not name.startswith("_"):
                name = f"_{name}"
            self.dismiss(name)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Add-item modal
# ═══════════════════════════════════════════════════════════════════════════

class AddItemScreen(ModalScreen[Optional[str]]):
    """Modal to add a new item (column) with schema hints."""

    DEFAULT_CSS = """
    AddItemScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #add-item-dialog {
        width: 70;
        height: auto;
        max-height: 30;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #item-suggestions {
        height: auto;
        max-height: 12;
        margin-top: 1;
    }
    """

    BINDINGS = [
        Binding("tab", "autocomplete", "Autocomplete", show=False, priority=True),
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, hints: SchemaHints, category: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._hints = hints
        self._category = category

    def compose(self) -> ComposeResult:
        with Vertical(id="add-item-dialog"):
            yield Label(
                f"[bold bright_green]╔══ ADD ITEM to {self._category} ══╗[/]\n"
                "[dim]Type an item name. Suggestions from the mmCIF dictionary:[/]"
            )
            yield Input(placeholder="item_name", id="item-name-input")
            yield Static(id="item-suggestions")
            yield Label("[dim]Enter ↵ to add  │  Esc to cancel[/]")

    def on_mount(self) -> None:
        self.query_one("#item-name-input", Input).focus()
        self._show_suggestions("")

    @on(Input.Changed, "#item-name-input")
    def handle_change(self, event: Input.Changed) -> None:
        self._show_suggestions(event.value.strip())

    def on_key(self, event) -> None:
        if event.key == "tab":
            self.action_autocomplete()
            event.prevent_default()
            event.stop()

    def _top_item_suggestion(self, prefix: str) -> Optional[str]:
        matches = self._hints.match_items(self._category, prefix)[:12]
        if matches:
            return matches[0]
        return None

    def _show_suggestions(self, prefix: str) -> None:
        matches = self._hints.match_items(self._category, prefix)[:12]
        if matches:
            lines = "  ".join(f"[bright_cyan]{m}[/]" for m in matches)
            self.query_one("#item-suggestions", Static).update(
                f"[bold bright_yellow]⚡ SUGGESTIONS:[/]\n  {lines}"
            )
        else:
            self.query_one("#item-suggestions", Static).update(
                "[dim]No matching items in dictionary[/]"
            )

    def action_autocomplete(self) -> None:
        input_widget = self.query_one("#item-name-input", Input)
        prefix = input_widget.value.strip()
        suggestion = self._top_item_suggestion(prefix)
        if not suggestion:
            return
        input_widget.value = suggestion
        self._show_suggestions(suggestion)

    @on(Input.Submitted, "#item-name-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        name = event.value.strip()
        if name:
            self.dismiss(name)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Add-row modal
# ═══════════════════════════════════════════════════════════════════════════

class AddRowScreen(ModalScreen[Optional[Dict[str, str]]]):
    """Modal to add a new row with per-field inputs and enum hints."""

    DEFAULT_CSS = """
    AddRowScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #add-row-dialog {
        width: 80;
        height: auto;
        max-height: 40;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #row-fields {
        height: auto;
        max-height: 28;
    }
    .row-field-label {
        margin-top: 1;
        height: 1;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(
        self,
        hints: SchemaHints,
        category: str,
        item_names: List[str],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._hints = hints
        self._category = category
        self._item_names = item_names

    def compose(self) -> ComposeResult:
        with Vertical(id="add-row-dialog"):
            yield Label(
                f"[bold bright_green]╔══ ADD ROW to {self._category} ══╗[/]\n"
                "[dim]Fill in values for each item (Tab to move between fields):[/]"
            )
            with VerticalScroll(id="row-fields"):
                for item in self._item_names:
                    enums = self._hints.enumerations_for(self._category, item)
                    hint_text = ""
                    if enums:
                        shown = enums[:6]
                        hint_text = f" [dim bright_magenta]({', '.join(shown)}{'…' if len(enums) > 6 else ''})[/]"
                    yield Label(
                        f"[bright_yellow]{item}[/]{hint_text}",
                        classes="row-field-label",
                    )
                    yield Input(
                        placeholder=f"value for {item}",
                        id=f"row-field-{item}",
                    )
            yield Label(
                "[dim]Enter ↵ on last field to add row  │  Esc to cancel[/]"
            )

    def on_mount(self) -> None:
        if self._item_names:
            self.query_one(f"#row-field-{self._item_names[0]}", Input).focus()

    @on(Input.Submitted)
    def handle_submit(self, event: Input.Submitted) -> None:
        # Find which field was submitted
        current_id = event.input.id or ""
        if not current_id.startswith("row-field-"):
            return
        current_item = current_id[len("row-field-"):]

        # If it's the last field, collect all values and dismiss
        if current_item == self._item_names[-1]:
            row_data: Dict[str, str] = {}
            for item in self._item_names:
                inp = self.query_one(f"#row-field-{item}", Input)
                row_data[item] = inp.value.strip() or "?"
            self.dismiss(row_data)
        else:
            # Move to next field
            idx = self._item_names.index(current_item)
            next_item = self._item_names[idx + 1]
            self.query_one(f"#row-field-{next_item}", Input).focus()

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Help screen
# ═══════════════════════════════════════════════════════════════════════════

class HelpScreen(ModalScreen):
    """Quick-reference overlay."""

    DEFAULT_CSS = """
    HelpScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #help-box {
        width: 72;
        height: auto;
        max-height: 36;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("question_mark", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-box"):
            yield Static(
                "[bold bright_green]"
                "╔════════════════════════════════════════════════════════════╗\n"
                "║                   SLOTH — QUICK REFERENCE                  ║\n"
                "╠════════════════════════════════════════════════════════════╣[/]\n"
                "[bright_cyan]"
                "  NAVIGATION\n"
                "  ──────────\n"
                "  ↑ / ↓       Move through tree or table rows\n"
                "  Enter       Select / expand node\n"
                "  Tab         Switch focus between panels\n"
                "\n"
                "  EDITING\n"
                "  ───────\n"
                "  a c         Add category to current block\n"
                "  a i         Add item (column) to current category\n"
                "  a r         Add row to current category\n"
                "  e           Edit selected cell value\n"
                "  d           Clear cell (set to ?) / delete row on # col\n"
                "  D           Delete entire column (item) under cursor\n"
                "  Ctrl+Z      Undo last change\n"
                "  Ctrl+Y      Redo\n"
                "\n"
                "  FILE\n"
                "  ────\n"
                "  Ctrl+S      Save mmCIF to file\n"
                "  Ctrl+E      Export as JSON\n"
                "  Ctrl+O      Open another file\n"
                "  Ctrl+N      New data block\n"
                "\n"
                "  OTHER\n"
                "  ─────\n"
                "  r           Show missing requirements\n"
                "  ?           Toggle this help\n"
                "  Ctrl+Q      Quit SLOTH\n"
                "[/]\n"
                "[bold bright_green]"
                "╚════════════════════════════════════════════════════════════╝[/]"
            )


class RequirementsScreen(ModalScreen):
    """Modal showing concrete schema/wwPDB gaps for the selected block."""

    DEFAULT_CSS = """
    RequirementsScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #requirements-box {
        width: 100;
        height: auto;
        max-height: 42;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #requirements-scroll {
        height: auto;
        max-height: 36;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("r", "dismiss", "Close"),
    ]

    # These come from EditorScreen._ISSUE_GROUP_* but we keep a local
    # copy so the screen stays self-contained.
    _GROUP_LABELS = {
        "missing": ("Missing required items", "bright_red"),
        "enum": ("Invalid enumeration values", "bright_yellow"),
        "type": ("Type / format mismatches", "yellow"),
        "pattern": ("Pattern violations", "yellow"),
        "value": ("Value constraint violations", "yellow"),
        "relationship": ("Relationship / foreign-key issues", "bright_magenta"),
        "other": ("Other issues", "dim white"),
    }
    _GROUP_ORDER = ["missing", "enum", "type", "pattern", "value", "relationship", "other"]
    _MAX_PER_GROUP = 8

    def __init__(
        self,
        block_name: str,
        methods: List[str],
        blocking: dict[str, List[str]],
        warnings_: dict[str, List[str]],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._block_name = block_name
        self._methods = methods
        self._blocking = blocking
        self._warnings = warnings_

    def _render_groups(
        self,
        groups: dict[str, List[str]],
        bullet_colour: str,
    ) -> str:
        if not groups:
            return f"  [dim]None.[/]"
        parts: List[str] = []
        for key in self._GROUP_ORDER:
            items = groups.get(key)
            if not items:
                continue
            label, colour = self._GROUP_LABELS.get(key, (key, "white"))
            parts.append(f"  [{colour} bold]{label}[/] [dim]({len(items)})[/]")
            shown = items[: self._MAX_PER_GROUP]
            for line in shown:
                # Truncate very long lines
                display = line if len(line) <= 90 else line[:87] + "..."
                parts.append(f"    [{bullet_colour}]•[/] {display}")
            remaining = len(items) - len(shown)
            if remaining > 0:
                parts.append(f"    [dim]… and {remaining} more[/]")
        return "\n".join(parts)

    def compose(self) -> ComposeResult:
        methods = ", ".join(self._methods) if self._methods else "(not set)"
        total_errors = sum(len(v) for v in self._blocking.values())
        total_warnings = sum(len(v) for v in self._warnings.values())

        header = (
            "[bold bright_green]"
            "╔══════════════════════════════════════════════════════════════════╗\n"
            f"║  Requirements — {self._block_name:<48} ║\n"
            "╠══════════════════════════════════════════════════════════════════╣[/]\n"
            f"  [dim]Method(s):[/] [bold]{methods}[/]\n"
            f"  [dim]Blocking:[/] [bold bright_red]{total_errors}[/]  "
            f"[dim]Warnings:[/] [bold bright_yellow]{total_warnings}[/]"
        )

        blocking_text = self._render_groups(self._blocking, "bright_red")
        warnings_text = self._render_groups(self._warnings, "bright_yellow")

        body = (
            f"\n\n[bold bright_red]▌ Blocking ({total_errors})[/]\n"
            f"{blocking_text}\n\n"
            f"[bold bright_yellow]▌ Warnings ({total_warnings})[/]\n"
            f"{warnings_text}\n\n"
            "[dim]Source: PDBx/mmCIF dictionary + wwPDB business rules.[/]\n"
            "[dim]Press [bold]Esc[/bold] or [bold]r[/bold] to close.[/]"
        )

        from textual.widgets import Static as _S
        from textual.containers import VerticalScroll

        with Vertical(id="requirements-box"):
            yield _S(header)
            with VerticalScroll(id="requirements-scroll"):
                yield _S(body)


# ═══════════════════════════════════════════════════════════════════════════
# Save-file modal
# ═══════════════════════════════════════════════════════════════════════════

class SaveFileScreen(ModalScreen[Optional[str]]):
    """Modal to choose save path."""

    DEFAULT_CSS = """
    SaveFileScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #save-dialog {
        width: 70;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, default_path: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self._default = default_path

    def compose(self) -> ComposeResult:
        with Vertical(id="save-dialog"):
            yield Label(
                "[bold bright_green]╔══ SAVE mmCIF ══╗[/]\n"
                "[dim]Enter output file path:[/]"
            )
            yield Input(
                value=self._default,
                placeholder="/path/to/output.cif",
                id="save-path-input",
            )
            yield Label("[dim]Enter ↵ to save  │  Esc to cancel[/]")

    def on_mount(self) -> None:
        self.query_one("#save-path-input", Input).focus()

    @on(Input.Submitted, "#save-path-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        path = event.value.strip()
        if path:
            self.dismiss(os.path.expanduser(path))

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Editor screen — the main workspace
# ═══════════════════════════════════════════════════════════════════════════

class EditorScreen(Screen):
    """Primary editing workspace with tree, table, hints, and completion meter."""

    BINDINGS = [
        Binding("ctrl+s", "save", "Save"),
        Binding("ctrl+e", "export_json", "Export JSON"),
        Binding("ctrl+o", "open_file", "Open"),
        Binding("ctrl+n", "new_block", "New block"),
        Binding("ctrl+z", "undo", "Undo"),
        Binding("ctrl+y", "redo", "Redo"),
        Binding("ctrl+q", "quit_app", "Quit"),
        Binding("question_mark", "help", "Help"),
        Binding("a", "add_menu", "Add…", show=True),
        Binding("e", "edit_cell", "Edit cell", show=True),
        Binding("d", "delete", "Delete cell/row", show=True),
        Binding("D", "delete_column", "Delete column", show=True),
        Binding("r", "show_requirements", "Requirements", show=True),
    ]

    DEFAULT_CSS = """
    EditorScreen {
        background: #0a0a0a;
    }

    #editor-header {
        dock: top;
        height: 1;
        background: $primary;
        color: $text;
        padding: 0 1;
    }

    #workspace {
        height: 1fr;
    }

    #table-panel {
        width: 3fr;
    }

    #data-table {
        height: 1fr;
    }

    #status-line {
        dock: bottom;
        height: 1;
        background: $primary;
        color: $text;
        padding: 0 1;
    }

    #inline-edit-input {
        dock: bottom;
        height: 3;
        display: none;
    }
    """

    def __init__(
        self,
        container: MMCIFDataContainer,
        file_path: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._container = container
        self._file_path = file_path
        self._handler = MMCIFHandler()
        self._hints = SchemaHints()
        self._validator: Optional[MMCIFValidator] = None
        self._current_block: Optional[str] = None
        self._current_category: Optional[str] = None
        self._inline_edit_ctx: Optional[Tuple[int, str]] = None
        self._paste_after_add_category = False
        self._paste_after_add_item = False
        self._dirty = False
        # Undo entry: (data_block_name, view_block, view_category, serialized_text)
        self._undo_stack: List[Tuple[Optional[str], Optional[str], Optional[str], str]] = []
        self._redo_stack: List[Tuple[Optional[str], Optional[str], Optional[str], str]] = []
        self._UNDO_LIMIT = 50

    # ── Undo / Redo ────────────────────────────────────────────────────

    def _snapshot_block(self, block_name: Optional[str] = None) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
        """Serialize a single block (or the entire container).

        Returns ``(data_block_name, view_block, view_category, text)``.
        """
        import io as _io
        from sloth.mmcif.writer import MMCIFWriter
        from sloth.mmcif.models import MMCIFDataContainer

        name = block_name or self._current_block
        if name and name in self._container.blocks:
            tmp = MMCIFDataContainer()
            tmp[name] = self._container[name]
        else:
            tmp = self._container
            name = None

        buf = _io.StringIO()
        MMCIFWriter().write(buf, tmp)
        return (name, self._current_block, self._current_category, buf.getvalue())

    def _push_undo(self) -> None:
        """Save current state to undo stack (call *before* a mutation)."""
        self._undo_stack.append(self._snapshot_block())
        if len(self._undo_stack) > self._UNDO_LIMIT:
            self._undo_stack.pop(0)
        self._redo_stack.clear()

    def _restore_snapshot(self, snap: Tuple[Optional[str], Optional[str], Optional[str], str]) -> None:
        """Replace the affected block (or entire container) from a snapshot."""
        import tempfile as _tmp, os as _os
        from sloth.mmcif.parser import MMCIFParser

        block_name, _view_block, _view_cat, text = snap
        fd, path = _tmp.mkstemp(suffix=".cif")
        try:
            with _os.fdopen(fd, "w") as fh:
                fh.write(text)
            parsed = MMCIFParser().parse(path)
        finally:
            _os.unlink(path)

        if block_name and block_name in parsed.blocks:
            self._container[block_name] = parsed[block_name]
        else:
            self._container = parsed

    def _refresh_after_restore(self, snap: Tuple[Optional[str], Optional[str], Optional[str], str]) -> None:
        """Reload tree and table, restoring the view that was active before the change."""
        data_block, view_block, view_cat, _text = snap

        tree = self.query_one("#cif-tree", CIFTree)

        if data_block is None:
            # Full-container restore (e.g. new-block undo) → must rebuild tree
            expanded = tree.get_expanded_state()
            self._hints.build_hierarchy(self._container)
            tree.load_container(self._container, children_of=self._hints.children_of)
            tree.restore_expanded_state(
                expanded,
                select_block=view_block,
                select_category=view_cat,
            )
        else:
            # Block-level restore → just update tree node labels (no rebuild)
            if data_block in self._container.blocks:
                block = self._container[data_block]
                for cat_name in block.categories:
                    cat = block[cat_name]
                    tree.refresh_category_node(
                        data_block, cat_name, cat.row_count, len(cat)
                    )

        # Defer the table/meter refresh so it runs *after* any tree events
        # have settled — otherwise tree reflows clobber the table contents.
        self.call_later(self._deferred_restore_view, view_block, view_cat)

    def _deferred_restore_view(
        self, view_block: Optional[str], view_cat: Optional[str]
    ) -> None:
        """Re-show the view that was active before the undo/redo mutation."""
        if (view_block
                and view_block in self._container.blocks
                and view_cat
                and view_cat in self._container[view_block].categories):
            self._show_category(view_block, view_cat)
        elif view_block and view_block in self._container.blocks:
            self._current_block = view_block
            self._current_category = None
        else:
            self._current_block = None
            self._current_category = None
        self._update_completion()

    def action_undo(self) -> None:
        if not self._undo_stack:
            self.notify("Nothing to undo.", severity="warning")
            return
        self._redo_stack.append(self._snapshot_block(self._undo_stack[-1][0]))
        snap = self._undo_stack.pop()
        self._restore_snapshot(snap)
        self._dirty = bool(self._undo_stack)
        self._refresh_after_restore(snap)
        self.notify("[dim]Undo[/]")

    def action_redo(self) -> None:
        if not self._redo_stack:
            self.notify("Nothing to redo.", severity="warning")
            return
        self._undo_stack.append(self._snapshot_block(self._redo_stack[-1][0]))
        snap = self._redo_stack.pop()
        self._restore_snapshot(snap)
        self._dirty = True
        self._refresh_after_restore(snap)
        self.notify("[dim]Redo[/]")

    def compose(self) -> ComposeResult:
        mode = "EDIT" if self._file_path else "BUILD"
        fname = Path(self._file_path).name if self._file_path else "new file"
        yield Static(
            f"[bold] SLOTH mmCIF Editor [/] │ "
            f"[bright_yellow]{mode}[/] │ "
            f"[dim]{fname}[/]",
            id="editor-header",
        )
        yield CompletionMeter()
        with Horizontal(id="workspace"):
            yield CIFTree(id="cif-tree")
            with Vertical(id="table-panel"):
                yield DataTable(id="data-table", cursor_type="cell")
        yield HintPanel(id="hint-panel")
        yield Input(id="inline-edit-input", placeholder="Edit cell value")
        yield Static(
            "[dim]? Help │ r Requirements │ a Add │ e Edit │ d Clear/Del │ D Del column │ Ctrl+Z Undo │ Ctrl+S Save[/]",
            id="status-line",
        )

    def on_mount(self) -> None:
        self._hints.build_hierarchy(self._container)
        tree = self.query_one("#cif-tree", CIFTree)
        tree.set_root_label(self._file_path or "(unsaved) new file")
        tree.load_container(self._container, children_of=self._hints.children_of)
        self._update_completion()

        # Auto-select first block/category if available
        for block in self._container:
            self._current_block = block.name
            for cat_name in block.categories:
                self._show_category(block.name, cat_name)
                return
            break

    # ── Tree events ────────────────────────────────────────────────────

    @on(CIFTree.CategorySelected)
    def on_category_selected(self, event: CIFTree.CategorySelected) -> None:
        self._show_category(event.block_name, event.category_name)

    @on(CIFTree.BlockSelected)
    def on_block_selected(self, event: CIFTree.BlockSelected) -> None:
        self._current_block = event.block_name
        self._current_category = None
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        block = self._container[event.block_name]
        cats = list(block.categories)
        hint = self.query_one("#hint-panel", HintPanel)
        hint.show_message(
            f"[bold bright_cyan]📦 {event.block_name}[/]  │  "
            f"[dim]{len(cats)} categories[/]  │  "
            f"[dim]Select a category to view data, [bold]a[/] to add, [bold]r[/] for requirements[/]"
        )

    @on(CIFTree.ItemSelected)
    def on_item_selected(self, event: CIFTree.ItemSelected) -> None:
        self._show_category(event.block_name, event.category_name)

        # Jump cursor to the selected item column in the table.
        table = self.query_one("#data-table", DataTable)
        block = self._container[event.block_name]
        cat = block[event.category_name]
        item_names = list(cat.items)
        if event.item_name in item_names:
            col_idx = item_names.index(event.item_name) + 1  # +1 for row-number column
            row_idx = 0 if cat.row_count > 0 else None
            table.focus()
            table.move_cursor(row=row_idx, column=col_idx, animate=False, scroll=True)

        enums = self._hints.enumerations_for(event.category_name, event.item_name)
        hint = self.query_one("#hint-panel", HintPanel)
        hint.show_item_hint(event.category_name, event.item_name, enums)

    # ── Display helpers ────────────────────────────────────────────────

    def _show_category(self, block_name: str, category_name: str) -> None:
        self._current_block = block_name
        self._current_category = category_name

        block = self._container[block_name]
        cat = block[category_name]
        table = self.query_one("#data-table", DataTable)

        table.clear(columns=True)
        # Add a row-number column plus each item
        table.add_column("#", key="__row_num__")
        item_names = list(cat.items)
        for item_name in item_names:
            table.add_column(item_name, key=item_name)

        # Populate rows
        for i in range(cat.row_count):
            row = cat[i]
            cells = [str(i)]
            for item_name in item_names:
                cells.append(row.data.get(item_name, "?"))
            table.add_row(*cells, key=str(i))

        # Show hint
        known_items = self._hints.items_for_category(category_name)
        hint = self.query_one("#hint-panel", HintPanel)
        hint.show_category_hint(category_name, known_items, item_names)

        self._update_completion()

    def _update_completion(self) -> None:
        meter = self.query_one(CompletionMeter)
        if not self._current_block:
            meter.update_stats("—", 0, 0, 0)
            return
        block = self._container[self._current_block]
        total_possible = 0
        total_filled = 0
        for cat_name in block.categories:
            schema_items = self._hints.items_for_category(cat_name)
            cat = block[cat_name]
            used = set(cat.items)
            if schema_items:
                total_possible += len(schema_items)
                total_filled += len(used & set(schema_items))
            else:
                total_possible += len(used)
                total_filled += len(used)
        meter.update_stats(
            self._current_block,
            total_filled,
            total_possible,
            len(list(block.categories)),
        )

    def _get_validator(self) -> Optional[MMCIFValidator]:
        if self._validator is None:
            try:
                self._validator = MMCIFValidator(quiet=True)
            except Exception:
                self._validator = None
        return self._validator

    def _experimental_methods_for_block(self, block: DataBlock) -> List[str]:
        if "_exptl" not in block.categories:
            return []
        try:
            methods = [
                m
                for m in block["_exptl"]["method"]
                if m not in ("?", ".", "")
            ]
        except Exception:
            return []
        return sorted(set(methods))

    @staticmethod
    def _classify_issue(msg: str) -> str:
        """Return a group key for a validation issue message."""
        low = msg.lower()
        if "mandatory" in low and ("missing" in low or "null" in low):
            return "missing"
        if "at least one of" in low:
            return "missing"
        if "expected at least" in low and "rows" in low:
            return "missing"
        if "not in allowed values" in low or "not allowed when" in low \
                or "not in enumeration" in low:
            return "enum"
        if "does not match expected type" in low:
            return "type"
        if "foreign key" in low or "parent category" in low or "composite key" in low:
            return "relationship"
        if "does not match" in low:
            return "pattern"
        if "length" in low or "value" in low:
            return "value"
        return "other"

    _ISSUE_GROUP_LABELS = {
        "missing": ("Missing required items", "bright_red"),
        "enum": ("Invalid enumeration values", "bright_yellow"),
        "type": ("Type / format mismatches", "yellow"),
        "pattern": ("Pattern violations", "yellow"),
        "value": ("Value constraint violations", "yellow"),
        "relationship": ("Relationship / foreign-key issues", "bright_magenta"),
        "other": ("Other issues", "dim white"),
    }
    _ISSUE_GROUP_ORDER = ["missing", "enum", "type", "pattern", "value", "relationship", "other"]

    def _requirement_gaps_for_block(
        self, block: DataBlock
    ) -> tuple[List[str], dict[str, List[str]], dict[str, List[str]]]:
        validator = self._get_validator()
        methods = self._experimental_methods_for_block(block)
        # Normalize method strings: strip surrounding quotes
        methods = [m.strip("'\"") for m in methods]
        if validator is None:
            return methods, {"other": ["Unable to initialize MMCIFValidator."]}, {}

        report = validator.validate(block)
        blocking: dict[str, List[str]] = {}
        warnings_: dict[str, List[str]] = {}

        for issue in report.errors:
            path = issue.path or ""
            text = f"{path}: {issue.message}" if path else issue.message
            group = self._classify_issue(issue.message)
            blocking.setdefault(group, []).append(text)
        for issue in report.warnings:
            path = issue.path or ""
            text = f"{path}: {issue.message}" if path else issue.message
            group = self._classify_issue(issue.message)
            warnings_.setdefault(group, []).append(text)

        # Deduplicate within groups
        for d in (blocking, warnings_):
            for k in d:
                d[k] = sorted(set(d[k]))

        return methods, blocking, warnings_

    def action_show_requirements(self) -> None:
        if not self._current_block:
            self.notify("Select a data block first.", severity="warning")
            return
        block = self._container[self._current_block]
        methods, blocking, warnings_ = self._requirement_gaps_for_block(block)
        self.app.push_screen(
            RequirementsScreen(
                block_name=self._current_block,
                methods=methods,
                blocking=blocking,
                warnings_=warnings_,
            )
        )

    # ── Actions: Add ───────────────────────────────────────────────────

    def action_add_menu(self) -> None:
        """Context-aware add action based on current tree node level."""
        tree = self.query_one("#cif-tree", CIFTree)
        node_data = None
        try:
            node = tree.cursor_node
            node_data = node.data if node is not None else None
        except Exception:
            node_data = None

        ntype = node_data.get("type") if isinstance(node_data, dict) else None

        if ntype == "item":
            # Column-level context: bulk paste values into this column.
            self._current_block = node_data.get("block", self._current_block)
            self._current_category = node_data.get("category", self._current_category)
            self.action_paste_column()
            return

        if ntype == "category":
            self._current_block = node_data.get("block", self._current_block)
            self._current_category = node_data.get("name", self._current_category)
            self.app.push_screen(
                _AddChoiceScreen(self._current_category),
                self._handle_add_choice,
            )
            return

        if ntype == "block":
            self._current_block = node_data.get("name", self._current_block)
            self._current_category = None
            self._do_add_category(paste_after=True)
            return

        # Root/container context (or fallback when no node info available).
        if self._current_category:
            self.app.push_screen(
                _AddChoiceScreen(self._current_category),
                self._handle_add_choice,
            )
            return
        if self._current_block:
            self._do_add_category(paste_after=True)
            return
        self.action_new_block()

    def _handle_add_choice(self, choice: Optional[str]) -> None:
        if choice == "category":
            self._do_add_category(paste_after=True)
        elif choice == "item":
            self._do_add_item(paste_after=True)
        elif choice == "row":
            self._do_add_row()
        elif choice == "paste_table":
            self.action_paste_table()
        elif choice == "paste_column":
            self.action_paste_column()

    def _do_add_category(self, paste_after: bool = False) -> None:
        self._paste_after_add_category = paste_after
        present_categories: Set[str] = set()
        if self._current_block:
            try:
                # __getitem__ accepts both "DEMO" and "data_DEMO".
                present_categories = set(self._container[self._current_block].categories)
            except KeyError:
                present_categories = set()
        self.app.push_screen(
            AddCategoryScreen(self._hints, present_categories=present_categories),
            self._on_category_added,
        )

    def _on_category_added(self, cat_name: Optional[str]) -> None:
        if cat_name is None or self._current_block is None:
            return
        self._push_undo()
        block = self._container[self._current_block]
        cat = Category(cat_name)

        # For known dictionary categories, pre-create all defined items
        # so users can start adding rows immediately.
        dict_items = self._hints.items_for_category(cat_name)
        for item_name in dict_items:
            cat[item_name] = Item(item_name, ['.'])

        block[cat_name] = cat
        self._dirty = True

        tree = self.query_one("#cif-tree", CIFTree)
        expanded = tree.get_expanded_state()
        self._hints.build_hierarchy(self._container)
        tree.load_container(self._container, children_of=self._hints.children_of)
        tree.restore_expanded_state(
            expanded,
            select_block=self._current_block,
            select_category=cat_name,
        )
        self._show_category(self._current_block, cat_name)
        if dict_items:
            self.notify(
                f"[bright_green]✓[/] Category {cat_name} added with {len(dict_items)} dictionary items!",
                severity="information",
            )
            if self._paste_after_add_category:
                self.action_paste_table()
        else:
            self.notify(f"[bright_green]✓[/] Category {cat_name} added!", severity="information")
            if self._paste_after_add_category:
                self.notify(
                    "No dictionary items found for this category; add items before table paste.",
                    severity="warning",
                )
        self._paste_after_add_category = False

    def _do_add_item(self, paste_after: bool = False) -> None:
        self._paste_after_add_item = paste_after
        if not self._current_category or not self._current_block:
            return
        self.app.push_screen(
            AddItemScreen(self._hints, self._current_category),
            self._on_item_added,
        )

    def _on_item_added(self, item_name: Optional[str]) -> None:
        if item_name is None or not self._current_block or not self._current_category:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]

        # Create an Item with '?' values matching existing row count
        row_count = cat.row_count
        values = ["?"] * row_count if row_count > 0 else []
        self._push_undo()
        cat[item_name] = Item(item_name, values)
        self._dirty = True

        self._show_category(self._current_block, self._current_category)
        tree = self.query_one("#cif-tree", CIFTree)
        tree.refresh_category_node(
            self._current_block,
            self._current_category,
            cat.row_count,
            len(cat),
        )
        self.notify(f"[bright_green]✓[/] Item {item_name} added!", severity="information")
        if self._paste_after_add_item:
            self._open_paste_column_for_item(item_name)
        self._paste_after_add_item = False

    # ── Paste-table ────────────────────────────────────────────────────

    def action_paste_table(self) -> None:
        """Open the paste-table modal for the current category."""
        if not self._current_category or not self._current_block:
            self.notify("Select a category first.", severity="warning")
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)
        if not item_names:
            self.notify("Category has no items; add items first.", severity="warning")
            return
        self.app.push_screen(
            PasteTableScreen(self._current_category, item_names),
            self._on_table_pasted,
        )

    def _on_table_pasted(self, text: Optional[str]) -> None:
        if not text or not self._current_block or not self._current_category:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)

        warn, data = self._parse_paste_text(text, item_names)
        if not data:
            self.notify(warn or "No data parsed.", severity="warning")
            return

        n_rows = max(len(v) for v in data.values())
        if n_rows == 0:
            return

        self._push_undo()
        for item_name in item_names:
            values = data.get(item_name, ['.'] * n_rows)
            cat[item_name] = Item(item_name, values)
        self._dirty = True

        self._show_category(self._current_block, self._current_category)
        tree = self.query_one("#cif-tree", CIFTree)
        tree.refresh_category_node(
            self._current_block, self._current_category, cat.row_count, len(cat)
        )
        msg = f"[bright_green]✓[/] Imported {n_rows} rows into {self._current_category}"
        if warn:
            msg += f"  [yellow]({warn})[/]"
        self.notify(msg, severity="information")

    def action_paste_column(self) -> None:
        """Open the paste-column modal for the currently selected item column."""
        if not self._current_category or not self._current_block:
            self.notify("Select a category first.", severity="warning")
            return

        table = self.query_one("#data-table", DataTable)
        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)
        if not item_names:
            self.notify("Category has no items; add items first.", severity="warning")
            return

        try:
            cursor_col = table.cursor_coordinate.column
        except Exception:
            cursor_col = 1

        if cursor_col <= 0 or cursor_col > len(item_names):
            self.notify(
                "Select a target item column first (or choose the item in the left panel).",
                severity="warning",
            )
            return

        target_item = item_names[cursor_col - 1]
        self._open_paste_column_for_item(target_item)

    def _open_paste_column_for_item(self, item_name: str) -> None:
        if not self._current_category or not self._current_block:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]
        self.app.push_screen(
            PasteColumnScreen(self._current_category, item_name, cat.row_count),
            lambda text, item=item_name: self._on_column_pasted(item, text),
        )

    def _on_column_pasted(self, item_name: str, text: Optional[str]) -> None:
        if text is None or not self._current_block or not self._current_category:
            return

        lines = text.splitlines()
        values = [line.strip() if line.strip() else "." for line in lines]
        if not values:
            self.notify("No values parsed.", severity="warning")
            return

        block = self._container[self._current_block]
        cat = block[self._current_category]
        row_count = cat.row_count

        # For populated categories, enforce exact row count to avoid accidental shifts.
        if row_count > 0 and len(values) != row_count:
            self.notify(
                f"Row mismatch: category has {row_count} rows, pasted {len(values)} values.",
                severity="warning",
            )
            return

        self._push_undo()
        if row_count == 0:
            item_names = list(cat.items)
            for existing_item in item_names:
                if existing_item == item_name:
                    cat[existing_item] = Item(existing_item, values)
                else:
                    cat[existing_item] = Item(existing_item, ['.'] * len(values))
        else:
            cat[item_name] = Item(item_name, values)
        self._dirty = True

        self._show_category(self._current_block, self._current_category)
        tree = self.query_one("#cif-tree", CIFTree)
        tree.refresh_category_node(
            self._current_block, self._current_category, cat.row_count, len(cat)
        )
        self.notify(
            f"[bright_green]✓[/] Imported {len(values)} values into {item_name}",
            severity="information",
        )

    def _parse_paste_text(
        self, text: str, item_names: List[str]
    ) -> Tuple[Optional[str], Dict[str, List[str]]]:
        """Parse pasted TSV/CSV text into {item_name: [values]}.

        Returns (warning_or_None, data_dict).  Empty data_dict means failure.
        """
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            return "No data found in pasted text.", {}

        # Detect delimiter: prefer tab (spreadsheet), fall back to comma
        first_line = lines[0]
        delimiter = '\t' if '\t' in first_line else ','

        rows = [line.split(delimiter) for line in lines]

        # Header detection: ≥50 % of first-row cells match known item names?
        item_set_lower = {n.lower() for n in item_names}
        first_row = [c.strip() for c in rows[0]]
        header_hits = sum(1 for c in first_row if c.lower() in item_set_lower)
        has_header = bool(first_row) and header_hits >= max(1, len(first_row) * 0.5)

        warn: Optional[str] = None
        if has_header:
            col_map: Dict[int, str] = {}
            for col_idx, cell in enumerate(first_row):
                cell_l = cell.lower()
                for item in item_names:
                    if item.lower() == cell_l:
                        col_map[col_idx] = item
                        break
            data_rows = rows[1:]
            unknown = [c for c in first_row if c and c.lower() not in item_set_lower]
            if unknown:
                warn = f"Unknown columns ignored: {', '.join(unknown)}"
        else:
            # No header row — map columns by position to item order
            col_map = {i: item for i, item in enumerate(item_names)}
            data_rows = rows

        if not data_rows:
            return "No data rows found.", {}

        n_rows = len(data_rows)
        result: Dict[str, List[str]] = {item: ['.'] * n_rows for item in item_names}
        for row_idx, row in enumerate(data_rows):
            for col_idx, item in col_map.items():
                if col_idx < len(row):
                    val = row[col_idx].strip()
                    result[item][row_idx] = val if val else '.'

        return warn, result

    def _do_add_row(self) -> None:
        if not self._current_category or not self._current_block:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)
        if not item_names:
            self.notify(
                "Add at least one item (column) before adding rows.",
                severity="warning",
            )
            return
        self.app.push_screen(
            AddRowScreen(self._hints, self._current_category, item_names),
            self._on_row_added,
        )

    def _on_row_added(self, row_data: Optional[Dict[str, str]]) -> None:
        if row_data is None or not self._current_block or not self._current_category:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]

        self._push_undo()
        for item_name, value in row_data.items():
            existing = cat._items.get(item_name)
            if isinstance(existing, Item):
                existing.add_value(value)
            elif isinstance(existing, list):
                existing.append(value)
            else:
                cat[item_name] = Item(item_name, [value])
        self._dirty = True

        self._show_category(self._current_block, self._current_category)
        tree = self.query_one("#cif-tree", CIFTree)
        tree.refresh_category_node(
            self._current_block,
            self._current_category,
            cat.row_count,
            len(cat),
        )
        self.notify("[bright_green]✓ Row added![/]", severity="information")

    # ── Actions: Edit ──────────────────────────────────────────────────

    def action_edit_cell(self) -> None:
        table = self.query_one("#data-table", DataTable)
        if not self._current_category or not self._current_block:
            self.notify("Select a category first.", severity="warning")
            return
        try:
            cursor_row = table.cursor_coordinate.row
            cursor_col = table.cursor_coordinate.column
        except Exception:
            self.notify("Place cursor on a cell to edit.", severity="warning")
            return

        if cursor_col == 0:
            self.notify("Cannot edit row numbers.", severity="warning")
            return

        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)

        if cursor_col - 1 >= len(item_names):
            return
        item_name = item_names[cursor_col - 1]
        current_value = cat[item_name][cursor_row] if cursor_row < cat.row_count else "?"
        self._start_inline_edit(cursor_row, item_name, current_value)

    def _start_inline_edit(self, row_idx: int, item_name: str, current_value: str) -> None:
        """Open lightweight in-place editor for a specific cell."""
        self._inline_edit_ctx = (row_idx, item_name)
        editor = self.query_one("#inline-edit-input", Input)
        editor.placeholder = f"Edit {item_name}[{row_idx}] and press Enter (Esc to cancel)"
        editor.value = current_value
        editor.styles.display = "block"
        editor.focus()

    def _finish_inline_edit(self, *, commit: bool) -> None:
        editor = self.query_one("#inline-edit-input", Input)
        ctx = self._inline_edit_ctx
        value = editor.value

        editor.styles.display = "none"
        editor.value = ""
        self._inline_edit_ctx = None
        self.query_one("#data-table", DataTable).focus()

        if commit and ctx is not None:
            row_idx, item_name = ctx
            self._on_cell_edited(row_idx, item_name, value)

    @on(Input.Submitted, "#inline-edit-input")
    def _inline_edit_submitted(self, event: Input.Submitted) -> None:
        self._finish_inline_edit(commit=True)

    def on_key(self, event) -> None:
        focused = self.focused
        if (
            event.key == "escape"
            and isinstance(focused, Input)
            and focused.id == "inline-edit-input"
        ):
            self._finish_inline_edit(commit=False)
            event.prevent_default()
            event.stop()

    def _on_cell_edited(
        self, row_idx: int, item_name: str, new_value: Optional[str]
    ) -> None:
        if new_value is None or not self._current_block or not self._current_category:
            return
        block = self._container[self._current_block]
        cat = block[self._current_category]

        self._push_undo()
        item = cat._items.get(item_name)
        if isinstance(item, Item):
            if item._values is not None and row_idx < len(item._values):
                item._values[row_idx] = new_value
                if hasattr(item, "values"):
                    delattr(item, "values")
        elif isinstance(item, list):
            if row_idx < len(item):
                item[row_idx] = new_value

        self._dirty = True
        self._show_category(self._current_block, self._current_category)
        self.notify(
            f"[bright_green]✓[/] {item_name}[{row_idx}] = {new_value}",
            severity="information",
        )

    # ── Actions: Delete ────────────────────────────────────────────────

    def action_delete(self) -> None:
        """Clear a cell (set to '?') or delete a row when on the # column."""
        table = self.query_one("#data-table", DataTable)
        if not self._current_category or not self._current_block:
            self.notify("Select a category first.", severity="warning")
            return
        try:
            cursor_row = table.cursor_coordinate.row
            cursor_col = table.cursor_coordinate.column
        except Exception:
            self.notify("Place cursor on a cell.", severity="warning")
            return

        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)

        if cursor_col == 0:
            # Row-number column → delete the entire row
            if cat.row_count == 0:
                return
            self.app.push_screen(
                ConfirmScreen(
                    f"[bold bright_red]Delete row {cursor_row} from {self._current_category}?[/]\n"
                    "[dim]Ctrl+Z to undo afterwards.[/]",
                ),
                lambda choice, r=cursor_row, c=cat, ns=item_names: (
                    self._confirmed_delete_row(choice, c, r, ns)
                ),
            )
        else:
            # Data cell → confirm then null it
            if cursor_col - 1 >= len(item_names):
                return
            item_name = item_names[cursor_col - 1]
            current_val = "?"
            try:
                row = cat[cursor_row]
                current_val = row.data.get(item_name, "?")
            except Exception:
                pass
            if current_val in ("?", ".", ""):
                self.notify("[dim]Already empty.[/]")
                return
            self.app.push_screen(
                ConfirmScreen(
                    f"[bold bright_yellow]Nullify {item_name}\[{cursor_row}]?[/]\n"
                    f"[dim]Current value: {current_val!r} → ?[/]",
                ),
                lambda choice, r=cursor_row, iname=item_name, c=cat: (
                    self._confirmed_clear_cell(choice, c, r, iname)
                ),
            )

    def _confirmed_clear_cell(
        self, choice: Optional[str], cat: Category, row_idx: int, item_name: str
    ) -> None:
        if choice != "Yes":
            return
        self._push_undo()
        item = cat._items.get(item_name)
        if isinstance(item, Item):
            if item._values is not None and row_idx < len(item._values):
                item._values[row_idx] = "?"
                if hasattr(item, "values"):
                    delattr(item, "values")
        elif isinstance(item, list):
            if row_idx < len(item):
                item[row_idx] = "?"
        self._dirty = True
        self._show_category(self._current_block, self._current_category)
        self.notify(f"[dim]{item_name}\[{row_idx}] → ?[/]")

    def _confirmed_delete_row(
        self, choice: Optional[str], cat: Category, row_idx: int, item_names: List[str]
    ) -> None:
        if choice != "Yes":
            return
        self._push_undo()
        self._delete_row(cat, row_idx, item_names)

    def action_delete_column(self) -> None:
        """Delete the entire column (item) under the cursor."""
        table = self.query_one("#data-table", DataTable)
        if not self._current_category or not self._current_block:
            self.notify("Select a category first.", severity="warning")
            return
        try:
            cursor_col = table.cursor_coordinate.column
        except Exception:
            self.notify("Place cursor on a column.", severity="warning")
            return

        block = self._container[self._current_block]
        cat = block[self._current_category]
        item_names = list(cat.items)

        if cursor_col == 0:
            self.notify("Move to a data column to delete it.", severity="warning")
            return
        if cursor_col - 1 >= len(item_names):
            return

        col_name = item_names[cursor_col - 1]
        self.app.push_screen(
            ConfirmScreen(
                f"[bold bright_red]Delete entire column {col_name} "
                f"from {self._current_category}?[/]\n"
                "[dim]Ctrl+Z to undo afterwards.[/]",
            ),
            lambda choice, cn=col_name, c=cat: self._confirmed_delete_column(choice, c, cn),
        )

    def _delete_row(
        self, cat: Category, row_idx: int, item_names: List[str]
    ) -> None:
        for item_name in item_names:
            item = cat._items.get(item_name)
            if isinstance(item, Item):
                if item._values is not None and row_idx < len(item._values):
                    item._values.pop(row_idx)
                    if hasattr(item, "values"):
                        delattr(item, "values")
            elif isinstance(item, list):
                if row_idx < len(item):
                    item.pop(row_idx)
        cat._row_cache.clear()
        self._dirty = True
        self._show_category(self._current_block, self._current_category)
        self.notify(f"[bright_red]✗[/] Row {row_idx} deleted", severity="information")

    def _confirmed_delete_column(
        self, choice: Optional[str], cat: Category, col_name: str
    ) -> None:
        if choice != "Yes":
            return
        self._push_undo()
        cat.delete(col_name)
        self._dirty = True
        self._show_category(self._current_block, self._current_category)
        self.notify(
            f"[bright_red]✗[/] Column {col_name} deleted",
            severity="information",
        )

    # ── Actions: File ops ──────────────────────────────────────────────

    def action_save(self) -> None:
        default = self._file_path or ""
        self.app.push_screen(SaveFileScreen(default), self._do_save)

    def _do_save(self, path: Optional[str]) -> None:
        if path is None:
            return
        try:
            self._handler.write(self._container, path)
            self._file_path = path
            self._dirty = False
            self.query_one("#cif-tree", CIFTree).set_root_label(path)
            self.notify(
                f"[bold bright_green]★ SAVED[/] → {path}",
                severity="information",
            )
        except Exception as exc:
            self.notify(f"Save failed: {exc}", severity="error")

    def action_export_json(self) -> None:
        default = ""
        if self._file_path:
            default = str(Path(self._file_path).with_suffix(".json"))
        self.app.push_screen(SaveFileScreen(default), self._do_export)

    def _do_export(self, path: Optional[str]) -> None:
        if path is None:
            return
        try:
            self._handler.export(self._container, path)
            self.notify(
                f"[bold bright_green]★ EXPORTED[/] → {path}",
                severity="information",
            )
        except Exception as exc:
            self.notify(f"Export failed: {exc}", severity="error")

    def action_open_file(self) -> None:
        self.app.push_screen(FileOpenScreen(), self._on_file_opened)

    def _on_file_opened(self, path: Optional[str]) -> None:
        if path is None:
            return
        try:
            container = self._handler.read(path)
            self._container = container
            self._file_path = path
            self._dirty = False
            self._current_block = None
            self._current_category = None
            self._hints.build_hierarchy(container)
            tree = self.query_one("#cif-tree", CIFTree)
            tree.set_root_label(path)
            tree.load_container(container, children_of=self._hints.children_of)
            table = self.query_one("#data-table", DataTable)
            table.clear(columns=True)

            # Auto-select first category
            for block in container:
                self._current_block = block.name
                for cat_name in block.categories:
                    self._show_category(block.name, cat_name)
                    return
                break

            self.notify(
                f"[bold bright_green]★ LOADED[/] {path}",
                severity="information",
            )
        except Exception as exc:
            self.notify(f"Failed to open: {exc}", severity="error")

    def action_new_block(self) -> None:
        self.app.push_screen(NewBlockScreen(), self._on_new_block)

    def _on_new_block(self, name: Optional[str]) -> None:
        if name is None:
            return
        block = DataBlock(name)
        block_key = f"data_{name}" if not name.startswith("data_") else name
        # Full-container snapshot since we're adding a new block
        self._undo_stack.append(self._snapshot_block(None))
        if len(self._undo_stack) > self._UNDO_LIMIT:
            self._undo_stack.pop(0)
        self._redo_stack.clear()
        self._container[block_key] = block
        self._dirty = True
        self._current_block = block.name
        self._current_category = None
        tree = self.query_one("#cif-tree", CIFTree)
        tree.add_block_node(block.name)
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        self._update_completion()
        self.notify(
            f"[bold bright_green]★ NEW BLOCK[/] data_{name}",
            severity="information",
        )

    def action_help(self) -> None:
        self.app.push_screen(HelpScreen())

    def action_quit_app(self) -> None:
        if self._dirty:
            self.app.push_screen(
                ConfirmScreen(
                    "[bold bright_yellow]You have unsaved changes.[/]\n"
                    "Save before quitting?",
                    buttons=("Save", "Discard", "Cancel"),
                ),
                self._handle_quit_confirm,
            )
        else:
            self.app.exit()

    def _handle_quit_confirm(self, choice: Optional[str]) -> None:
        if choice == "Save":
            default = self._file_path or ""
            self.app.push_screen(
                SaveFileScreen(default),
                self._save_then_quit,
            )
        elif choice == "Discard":
            self.app.exit()
        # "Cancel" or None → stay

    def _save_then_quit(self, path: Optional[str]) -> None:
        if path is None:
            return  # user cancelled the save dialog
        try:
            self._handler.write(self._container, path)
            self.app.exit()
        except Exception as exc:
            self.notify(f"Save failed: {exc}", severity="error")


# ═══════════════════════════════════════════════════════════════════════════
# Confirmation modal
# ═══════════════════════════════════════════════════════════════════════════

class ConfirmScreen(ModalScreen[Optional[str]]):
    """Generic confirmation dialog with focusable buttons.

    Arrow keys (left/right) and Tab/Shift+Tab move between buttons.
    Enter activates the focused button.  Esc cancels.

    Returns the label string of the chosen button, or ``None`` on Esc.
    """

    DEFAULT_CSS = """
    ConfirmScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #confirm-box {
        width: 60;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #confirm-buttons {
        margin-top: 1;
        height: auto;
        align: center middle;
    }
    #confirm-buttons Button {
        margin: 0 1;
        min-width: 12;
    }
    #confirm-buttons Button:focus {
        background: $primary;
        color: $text;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(
        self,
        prompt: str,
        buttons: Sequence[str] = ("Yes", "No"),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._prompt = prompt
        self._buttons = list(buttons)

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-box"):
            yield Static(self._prompt)
            with Horizontal(id="confirm-buttons"):
                for i, label in enumerate(self._buttons):
                    yield Button(label, id=f"confirm-btn-{i}")

    def on_mount(self) -> None:
        self.query_one("#confirm-btn-0", Button).focus()

    def on_key(self, event) -> None:
        if event.key in ("left", "right"):
            btns = list(self.query(Button))
            focused = self.focused
            if focused in btns:
                idx = btns.index(focused)
                nxt = (idx - 1) if event.key == "left" else (idx + 1)
                btns[nxt % len(btns)].focus()
            event.prevent_default()
            event.stop()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.label.plain)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Edit-cell modal
# ═══════════════════════════════════════════════════════════════════════════

class EditCellScreen(ModalScreen[Optional[str]]):
    """Modal for editing a single cell value with enum hints."""

    DEFAULT_CSS = """
    EditCellScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #edit-cell-dialog {
        width: 60;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(
        self,
        category: str,
        item: str,
        current: str,
        enumerations: List[str],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._category = category
        self._item = item
        self._current = current
        self._enumerations = enumerations

    def compose(self) -> ComposeResult:
        with Vertical(id="edit-cell-dialog"):
            yield Label(
                f"[bold bright_green]╔══ EDIT {self._category}.{self._item} ══╗[/]"
            )
            if self._enumerations:
                shown = self._enumerations[:8]
                vals = ", ".join(f"[bright_magenta]{v}[/]" for v in shown)
                suffix = "…" if len(self._enumerations) > 8 else ""
                yield Label(f"[dim]Allowed values:[/] {vals}{suffix}")
            yield Input(value=self._current, id="edit-cell-input")
            yield Label("[dim]Enter ↵ to confirm  │  Esc to cancel[/]")

    def on_mount(self) -> None:
        self.query_one("#edit-cell-input", Input).focus()

    @on(Input.Submitted, "#edit-cell-input")
    def handle_submit(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Paste-table modal
# ═══════════════════════════════════════════════════════════════════════════

class PasteTableScreen(ModalScreen[Optional[str]]):
    """Modal to paste TSV/CSV rows into the current category."""

    DEFAULT_CSS = """
    PasteTableScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #paste-dialog {
        width: 90;
        height: 30;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #paste-area {
        height: 1fr;
        margin: 1 0;
        border: solid $accent;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, category: str, item_names: List[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self._category = category
        self._item_names = item_names

    def compose(self) -> ComposeResult:
        cols = "  ".join(f"[bright_cyan]{n}[/]" for n in self._item_names[:8])
        if len(self._item_names) > 8:
            cols += "  [dim]…[/]"
        with Vertical(id="paste-dialog"):
            yield Label(
                f"[bold bright_green]╔══ PASTE TABLE into {self._category} ══╗[/]\n"
                f"[dim]Columns:[/] {cols}\n"
                "[dim]TSV/CSV — first row may be headers. "
                "Ctrl+S to import  │  Esc to cancel[/]"
            )
            yield TextArea(id="paste-area")

    def on_mount(self) -> None:
        self.query_one("#paste-area", TextArea).focus()

    def on_key(self, event) -> None:
        if event.key == "ctrl+s":
            text = self.query_one("#paste-area", TextArea).text.strip()
            if text:
                self.dismiss(text)
            event.prevent_default()
            event.stop()

    def action_cancel(self) -> None:
        self.dismiss(None)


class PasteColumnScreen(ModalScreen[Optional[str]]):
    """Modal to paste one value per line into a selected item column."""

    DEFAULT_CSS = """
    PasteColumnScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #paste-col-dialog {
        width: 90;
        height: 26;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    #paste-col-area {
        height: 1fr;
        margin: 1 0;
        border: solid $accent;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, category: str, item_name: str, row_count: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self._category = category
        self._item_name = item_name
        self._row_count = row_count

    def compose(self) -> ComposeResult:
        count_hint = (
            f"[dim]Expecting exactly {self._row_count} lines.[/]"
            if self._row_count > 0
            else "[dim]Category is empty: line count will become row count.[/]"
        )
        with Vertical(id="paste-col-dialog"):
            yield Label(
                f"[bold bright_green]╔══ PASTE COLUMN into {self._category}.{self._item_name} ══╗[/]\n"
                "[dim]One value per line (blank lines become '.'). "
                "Ctrl+S to import  │  Esc to cancel[/]\n"
                f"{count_hint}"
            )
            yield TextArea(id="paste-col-area")

    def on_mount(self) -> None:
        self.query_one("#paste-col-area", TextArea).focus()

    def on_key(self, event) -> None:
        if event.key == "ctrl+s":
            text = self.query_one("#paste-col-area", TextArea).text
            self.dismiss(text)
            event.prevent_default()
            event.stop()

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Add-choice modal
# ═══════════════════════════════════════════════════════════════════════════

class _AddChoiceScreen(ModalScreen[Optional[str]]):
    """Quick picker: what to add?"""

    DEFAULT_CSS = """
    _AddChoiceScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #add-choice-dialog {
        width: 50;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("c", "pick_category", "Category"),
        Binding("i", "pick_item", "Item"),
        Binding("r", "pick_row", "Row"),
    ]

    def __init__(self, current_category: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._cat = current_category

    def compose(self) -> ComposeResult:
        with Vertical(id="add-choice-dialog"):
            yield Static(
                "[bold bright_green]╔══ ADD WHAT? ══╗[/]\n\n"
                f"  [bright_cyan]\\[C][/]ategory   — add category + paste table\n"
                f"  [bright_cyan]\\[I][/]tem       — add column + paste values\n"
                f"  [bright_cyan]\\[R][/]ow        — new row in {self._cat}\n"
                "\n"
                "[dim]Press key or Esc to cancel[/]"
            )

    def action_pick_category(self) -> None:
        self.dismiss("category")

    def action_pick_item(self) -> None:
        self.dismiss("item")

    def action_pick_row(self) -> None:
        self.dismiss("row")

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Main SLOTH App
# ═══════════════════════════════════════════════════════════════════════════

class SlothApp(App):
    """SLOTH — mmCIF Interactive Editor."""

    TITLE = "SLOTH"
    SUB_TITLE = "mmCIF Interactive Editor"

    CSS = """
    Screen {
        background: #0a0a0a;
    }
    Header {
        background: #114411;
        color: #44ff44;
    }
    Footer {
        background: #114411;
        color: #44ff44;
    }
    DataTable {
        background: #0a0a0a;
    }
    DataTable > .datatable--header {
        background: #114411;
        color: #44ff44;
        text-style: bold;
    }
    DataTable > .datatable--cursor {
        background: #226622;
        color: #ffffff;
    }
    DataTable > .datatable--even-row {
        background: #0d0d0d;
    }
    Tree {
        background: #0a0a0a;
        color: #44ff44;
        border-right: solid #226622;
    }
    Tree > .tree--cursor {
        background: #226622;
        color: #ffffff;
    }
    CompletionMeter {
        background: #0d0d0d;
        color: #44ff44;
        border-bottom: solid #226622;
    }
    HintPanel {
        background: #0d0d0d;
        color: #44ff44;
        border-top: solid #226622;
    }
    Input {
        background: #111111;
        color: #44ff44;
        border: solid #226622;
    }
    Input:focus {
        border: solid #44ff44;
    }
    Static {
        color: #44ff44;
    }
    Label {
        color: #44ff44;
    }
    #editor-header {
        background: #114411;
        color: #44ff44;
    }
    #status-line {
        background: #114411;
        color: #44ff44;
    }
    """

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit"),
    ]

    def __init__(self, file_path: Optional[str] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._initial_file = file_path

    def on_mount(self) -> None:
        if self._initial_file:
            self._load_file(self._initial_file)
        else:
            self.push_screen(SplashScreen())

    def _load_file(self, path: str) -> None:
        try:
            handler = MMCIFHandler()
            container = handler.read(path)
            self.push_screen(EditorScreen(container, file_path=path))
        except Exception as exc:
            self.push_screen(SplashScreen())
            self.notify(f"Failed to open: {exc}", severity="error")

    # -- callbacks from splash screen modals --

    def _handle_splash_open(self, path: Optional[str]) -> None:
        if path:
            self._load_file(path)

    def _handle_splash_new(self, name: Optional[str]) -> None:
        if name:
            block = DataBlock(name)
            container = MMCIFDataContainer(
                {name: block}, source_format=DataSourceFormat.MMCIF
            )
            self.push_screen(EditorScreen(container))


# ═══════════════════════════════════════════════════════════════════════════
# Override splash screen actions to route through the app
# ═══════════════════════════════════════════════════════════════════════════

# Patch SplashScreen to use app-level callbacks
_orig_splash_open = SplashScreen.action_open_file
_orig_splash_new = SplashScreen.action_new_file


def _splash_open_patched(self: SplashScreen) -> None:
    self.app.push_screen(FileOpenScreen(), self.app._handle_splash_open)


def _splash_new_patched(self: SplashScreen) -> None:
    self.app.push_screen(NewBlockScreen(), self.app._handle_splash_new)


SplashScreen.action_open_file = _splash_open_patched
SplashScreen.action_new_file = _splash_new_patched


# ═══════════════════════════════════════════════════════════════════════════
# CLI entry point
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Launch the SLOTH mmCIF editor."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="sloth",
        description="SLOTH — mmCIF Interactive Editor. Lazy by design. Fast by default.",
    )
    parser.add_argument(
        "file",
        nargs="?",
        default=None,
        help="Path to an mmCIF file to open",
    )
    args = parser.parse_args()

    app = SlothApp(file_path=args.file)
    app.run()


if __name__ == "__main__":
    main()
