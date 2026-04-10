"""
Custom widgets for the SLOTH CLI.

Retro mainframe-inspired components with an arcade flair.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from textual.widgets import Static, Tree, DataTable, Input, Footer, Header
from textual.widgets.tree import TreeNode
from textual.message import Message

from ..models import MMCIFDataContainer, DataBlock, Category


# ── ASCII art ──────────────────────────────────────────────────────────────

SLOTH_LOGO = r"""[bold bright_green]
  ███████╗██╗      ██████╗ ████████╗██╗  ██╗
  ██╔════╝██║     ██╔═══██╗╚══██╔══╝██║  ██║
  ███████╗██║     ██║   ██║   ██║   ███████║
  ╚════██║██║     ██║   ██║   ██║   ██╔══██║
  ███████║███████╗╚██████╔╝   ██║   ██║  ██║
  ╚══════╝╚══════╝ ╚═════╝    ╚═╝   ╚═╝  ╚═╝[/]
[dim bright_green]  ┌─────────────────────────────────────────┐
  │  Structural Loader with On-demand       │
  │  Traversal Handling                     │
  │                                         │
  │  ░░ mmCIF Interactive Editor ░░         │
  │  Lazy by design. Fast by default.       │
  └─────────────────────────────────────────┘[/]"""


SPLASH_ART = r"""[bold bright_green]
  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   ███████╗██╗      ██████╗ ████████╗██╗  ██╗                 ║
  ║   ██╔════╝██║     ██╔═══██╗╚══██╔══╝██║  ██║                 ║
  ║   ███████╗██║     ██║   ██║   ██║   ███████║                 ║
  ║   ╚════██║██║     ██║   ██║   ██║   ██╔══██║                 ║
  ║   ███████║███████╗╚██████╔╝   ██║   ██║  ██║                 ║
  ║   ╚══════╝╚══════╝ ╚═════╝    ╚═╝   ╚═╝  ╚═╝                ║
  ║                                                               ║
  ║       ░░ mmCIF Interactive Editor ░░                          ║
  ║                                                               ║
  ║   Lazy by design. Fast by default.                            ║
  ║                                                               ║
  ╠═══════════════════════════════════════════════════════════════╣
  ║                                                               ║
  ║   [O]  Open mmCIF file      — Browse or edit existing         ║
  ║   [N]  New mmCIF file       — Build from scratch              ║
  ║   [Q]  Quit                 — Exit SLOTH                      ║
  ║                                                               ║
  ╠═══════════════════════════════════════════════════════════════╣
  ║   Controls:  ↑↓ Navigate  │  Enter Select  │  ? Help         ║
  ╚═══════════════════════════════════════════════════════════════╝[/]"""


# ── Score / completion meter ───────────────────────────────────────────────

class CompletionMeter(Static):
    """Shows how 'complete' the current data block is based on schema coverage."""

    DEFAULT_CSS = """
    CompletionMeter {
        dock: top;
        height: 3;
        padding: 0 1;
        background: $surface;
        color: $text;
        border-bottom: solid $primary;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__("", **kwargs)
        self._block_name = ""
        self._filled = 0
        self._total = 0
        self._category_count = 0

    def update_stats(
        self, block_name: str, filled: int, total: int, category_count: int
    ) -> None:
        self._block_name = block_name
        self._filled = filled
        self._total = total
        self._category_count = category_count
        self._render_bar()

    def _render_bar(self) -> None:
        if self._total == 0:
            pct = 0
        else:
            pct = int(self._filled / self._total * 100)

        bar_width = 30
        filled_blocks = int(bar_width * pct / 100)
        empty_blocks = bar_width - filled_blocks

        bar = "█" * filled_blocks + "░" * empty_blocks

        if pct >= 80:
            colour = "bright_green"
            rank = "★★★ COMPLETE"
        elif pct >= 50:
            colour = "bright_yellow"
            rank = "★★☆ GOOD"
        elif pct >= 20:
            colour = "bright_red"
            rank = "★☆☆ PARTIAL"
        else:
            colour = "bright_red"
            rank = "☆☆☆ MINIMAL"

        self.update(
            f" [{colour}]▌{bar}▐[/] {pct}%  "
            f"[bold]{rank}[/]  │  "
            f"[dim]Block:[/] [bold]{self._block_name}[/]  │  "
            f"[dim]Categories:[/] [bold]{self._category_count}[/]  │  "
            f"[dim]Items filled:[/] [bold]{self._filled}[/]/{self._total}"
        )


# ── mmCIF tree navigator ──────────────────────────────────────────────────

class CIFTree(Tree):
    """Hierarchical tree for navigating mmCIF data blocks → categories → items."""

    class CategorySelected(Message):
        def __init__(self, block_name: str, category_name: str) -> None:
            super().__init__()
            self.block_name = block_name
            self.category_name = category_name

    class BlockSelected(Message):
        def __init__(self, block_name: str) -> None:
            super().__init__()
            self.block_name = block_name

    class ItemSelected(Message):
        def __init__(self, block_name: str, category_name: str, item_name: str) -> None:
            super().__init__()
            self.block_name = block_name
            self.category_name = category_name
            self.item_name = item_name

    DEFAULT_CSS = """
    CIFTree {
        width: 1fr;
        min-width: 30;
        max-width: 50;
        background: $surface;
        border-right: solid $primary;
        scrollbar-size: 1 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__("🦥 mmCIF", **kwargs)
        self._block_nodes: Dict[str, TreeNode] = {}
        self._container: Optional[MMCIFDataContainer] = None

    def load_container(self, container: MMCIFDataContainer) -> None:
        self._container = container
        self.clear()
        self._block_nodes.clear()

        for block in container:
            bnode = self.root.add(
                f"[bold bright_cyan]📦 {block.name}[/]",
                data={"type": "block", "name": block.name},
            )
            self._block_nodes[block.name] = bnode

            for cat_name in sorted(block.categories):
                cat = block[cat_name]
                row_count = cat.row_count
                label = (
                    f"[bright_yellow]▸ {cat_name}[/]"
                    f" [dim]({row_count} rows, {len(cat)} items)[/]"
                )
                cnode = bnode.add(
                    label,
                    data={
                        "type": "category",
                        "block": block.name,
                        "name": cat_name,
                    },
                )
                for item_name in cat.items:
                    cnode.add_leaf(
                        f"[dim bright_green]  ⬦ {item_name}[/]",
                        data={
                            "type": "item",
                            "block": block.name,
                            "category": cat_name,
                            "item": item_name,
                        },
                    )
            bnode.expand()

        self.root.expand()

    def add_block_node(self, block_name: str) -> None:
        bnode = self.root.add(
            f"[bold bright_cyan]📦 {block_name}[/]",
            data={"type": "block", "name": block_name},
        )
        self._block_nodes[block_name] = bnode
        bnode.expand()
        self.root.expand()

    def add_category_node(self, block_name: str, cat_name: str, row_count: int, item_count: int) -> None:
        bnode = self._block_nodes.get(block_name)
        if bnode is None:
            return
        label = (
            f"[bright_yellow]▸ {cat_name}[/]"
            f" [dim]({row_count} rows, {item_count} items)[/]"
        )
        cnode = bnode.add(
            label,
            data={
                "type": "category",
                "block": block_name,
                "name": cat_name,
            },
        )
        return cnode

    def refresh_category_node(
        self, block_name: str, cat_name: str, row_count: int, item_count: int
    ) -> None:
        """Re-render a category node label after edits."""
        bnode = self._block_nodes.get(block_name)
        if bnode is None:
            return
        for child in bnode.children:
            if child.data and child.data.get("name") == cat_name:
                child.set_label(
                    f"[bright_yellow]▸ {cat_name}[/]"
                    f" [dim]({row_count} rows, {item_count} items)[/]"
                )
                break

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node_data = event.node.data
        if node_data is None:
            return
        ntype = node_data.get("type")
        if ntype == "block":
            self.post_message(self.BlockSelected(node_data["name"]))
        elif ntype == "category":
            self.post_message(
                self.CategorySelected(node_data["block"], node_data["name"])
            )
        elif ntype == "item":
            self.post_message(
                self.ItemSelected(
                    node_data["block"], node_data["category"], node_data["item"]
                )
            )


# ── Hint panel ─────────────────────────────────────────────────────────────

class HintPanel(Static):
    """Shows contextual schema hints — valid items, enumerations, completions."""

    DEFAULT_CSS = """
    HintPanel {
        dock: bottom;
        height: auto;
        max-height: 10;
        padding: 0 1;
        background: $surface;
        border-top: solid $primary;
        color: $text;
    }
    """

    def show_category_hint(self, category: str, known_items: List[str], used_items: List[str]) -> None:
        unused = sorted(set(known_items) - set(used_items))
        if not unused:
            self.update(
                f"[bold bright_green]✓[/] [dim]All known items present in[/] "
                f"[bold]{category}[/]"
            )
            return
        hint_items = unused[:12]
        remaining = len(unused) - len(hint_items)
        items_str = "  ".join(f"[bright_cyan]{i}[/]" for i in hint_items)
        suffix = f"  [dim]… and {remaining} more[/]" if remaining > 0 else ""
        self.update(
            f"[bold bright_yellow]⚡ AVAILABLE ITEMS[/] for [bold]{category}[/]:\n"
            f"  {items_str}{suffix}"
        )

    def show_item_hint(self, category: str, item: str, enumerations: List[str]) -> None:
        if enumerations:
            vals = "  ".join(f"[bright_magenta]{v}[/]" for v in enumerations[:15])
            remaining = len(enumerations) - 15
            suffix = f"  [dim]… +{remaining}[/]" if remaining > 0 else ""
            self.update(
                f"[bold bright_yellow]⚡ ALLOWED VALUES[/] for "
                f"[bold]{category}.{item}[/]:\n  {vals}{suffix}"
            )
        else:
            self.update(
                f"[dim]Item[/] [bold]{category}.{item}[/] — [dim]free text (no enumeration)[/]"
            )

    def show_message(self, msg: str) -> None:
        self.update(msg)


# ── Command input ──────────────────────────────────────────────────────────

class CommandInput(Input):
    """Retro command-line input with a prompt prefix."""

    DEFAULT_CSS = """
    CommandInput {
        dock: bottom;
        height: 3;
        border-top: solid $primary;
        background: $surface;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            placeholder="Type command or value… (? for help)",
            **kwargs,
        )
