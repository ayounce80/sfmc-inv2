"""Selection screen for choosing object types to extract.

Provides tabbed interface with presets and custom selection,
using Textual widgets for modern TUI experience.
"""

from typing import Any, Callable, Optional

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Label,
    SelectionList,
    Static,
    TabbedContent,
    TabPane,
)
from textual.widgets.selection_list import Selection

from ..extractors import EXTRACTORS
from ..orchestration import PRESETS
from .config_store import get_config_store


# Object type metadata for display
OBJECT_TYPES = [
    # Existing extractors
    ("automations", "Automations", "Scheduled workflows with steps and activities"),
    ("data_extensions", "Data Extensions", "Custom data tables with fields"),
    ("queries", "Queries", "SQL Query Activities"),
    ("journeys", "Journeys", "Journey Builder journeys"),
    # Phase 1 - Automation Activities (REST)
    ("scripts", "Scripts", "SSJS Script Activities"),
    ("imports", "Imports", "Import File Activities"),
    ("data_extracts", "Data Extracts", "Data Extract Activities"),
    ("filters", "Filters", "Filter Activities"),
    ("file_transfers", "File Transfers", "File Transfer Activities"),
    # Phase 2 - Content & Structure (REST)
    ("assets", "Assets", "Content Builder assets (emails, blocks, etc.)"),
    ("folders", "Folders", "Automation Studio folders"),
    ("event_definitions", "Event Definitions", "Journey entry event definitions"),
    # Phase 3 - Messaging Objects (SOAP)
    ("classic_emails", "Classic Emails", "Classic email definitions (non-Content Builder)"),
    ("triggered_sends", "Triggered Sends", "Triggered Send Definitions"),
    ("lists", "Lists", "Subscriber Lists"),
    ("sender_profiles", "Sender Profiles", "Email sender profiles"),
    ("delivery_profiles", "Delivery Profiles", "Email delivery profiles"),
    ("send_classifications", "Send Classifications", "Send classification definitions"),
    ("templates", "Templates", "Classic email templates"),
    ("account", "Account", "Account/Business Unit information"),
]


class SelectionScreen(Screen):
    """Screen for selecting object types to extract."""

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("enter", "confirm", "Start Extraction"),
        ("a", "select_all", "Select All"),
        ("n", "select_none", "Select None"),
    ]

    CSS = """
    SelectionScreen {
        background: $surface;
    }

    #main-container {
        width: 100%;
        height: 100%;
        padding: 1 2;
    }

    #selection-container {
        height: 1fr;
        margin-bottom: 1;
    }

    .preset-button {
        margin: 0 1;
    }

    #preset-bar {
        height: 3;
        padding: 0 1;
        align: center middle;
    }

    #selection-list {
        height: 1fr;
        border: solid $primary;
        padding: 1;
    }

    #summary-bar {
        height: 3;
        padding: 0 1;
        align: center middle;
        background: $surface-darken-1;
    }

    #button-bar {
        height: 3;
        align: center middle;
    }

    .option-checkbox {
        margin: 0 2;
    }

    #options-container {
        height: auto;
        padding: 1;
    }
    """

    def __init__(
        self,
        on_confirm: Optional[Callable[[list[str], dict[str, Any]], None]] = None,
        **kwargs: Any,
    ):
        """Initialize the selection screen.

        Args:
            on_confirm: Callback when selection is confirmed.
        """
        super().__init__(**kwargs)
        self._on_confirm = on_confirm
        self._config = get_config_store()

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()

        with Container(id="main-container"):
            # Preset buttons
            with Horizontal(id="preset-bar"):
                yield Label("Presets: ")
                for preset_name, preset_data in PRESETS.items():
                    yield Button(
                        preset_name.title(),
                        id=f"preset-{preset_name}",
                        classes="preset-button",
                    )

            # Selection list
            with Container(id="selection-container"):
                yield SelectionList[str](
                    *[
                        Selection(
                            f"{name.replace('_', ' ').title()} - {desc}",
                            key,
                            initial_state=key in self._get_initial_selection(),
                        )
                        for key, name, desc in OBJECT_TYPES
                    ],
                    id="selection-list",
                )

            # Options
            with Horizontal(id="options-container"):
                yield Checkbox(
                    "Include details",
                    id="opt-details",
                    value=self._config.get_include_details(),
                    classes="option-checkbox",
                )
                yield Checkbox(
                    "Include content (SQL/scripts)",
                    id="opt-content",
                    value=self._config.get_include_content(),
                    classes="option-checkbox",
                )

            # Summary
            with Horizontal(id="summary-bar"):
                yield Static("Selected: 0 object types", id="selection-summary")

            # Buttons
            with Horizontal(id="button-bar"):
                yield Button("Start Extraction", id="btn-start", variant="primary")
                yield Button("Cancel", id="btn-cancel")

        yield Footer()

    def on_mount(self) -> None:
        """Called when screen is mounted."""
        self._update_summary()

    def _get_initial_selection(self) -> list[str]:
        """Get initial selection from config or default."""
        last = self._config.get_last_selection()
        if last:
            return last
        # Default to quick preset
        return PRESETS.get("quick", {}).get("extractors", ["automations"])

    def _update_summary(self) -> None:
        """Update the selection summary."""
        selection_list = self.query_one("#selection-list", SelectionList)
        selected = selection_list.selected
        count = len(selected)

        summary = self.query_one("#selection-summary", Static)
        summary.update(f"Selected: {count} object type{'s' if count != 1 else ''}")

    def on_selection_list_selected_changed(
        self, event: SelectionList.SelectedChanged
    ) -> None:
        """Handle selection changes."""
        self._update_summary()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        button_id = event.button.id

        if button_id == "btn-start":
            self._confirm_selection()
        elif button_id == "btn-cancel":
            self.app.exit()
        elif button_id and button_id.startswith("preset-"):
            preset_name = button_id.replace("preset-", "")
            self._apply_preset(preset_name)

    def _apply_preset(self, preset_name: str) -> None:
        """Apply a preset selection."""
        preset = PRESETS.get(preset_name)
        if not preset:
            return

        selection_list = self.query_one("#selection-list", SelectionList)
        extractors = preset.get("extractors", [])

        # Clear and set selection
        for i, (key, _, _) in enumerate(OBJECT_TYPES):
            if key in extractors:
                selection_list.select(key)
            else:
                selection_list.deselect(key)

        self._update_summary()

    def _confirm_selection(self) -> None:
        """Confirm selection and proceed."""
        selection_list = self.query_one("#selection-list", SelectionList)
        selected = list(selection_list.selected)

        if not selected:
            self.notify("Please select at least one object type", severity="warning")
            return

        # Get options
        include_details = self.query_one("#opt-details", Checkbox).value
        include_content = self.query_one("#opt-content", Checkbox).value

        # Save preferences
        self._config.set_last_selection(selected)
        self._config.set_include_details(include_details)
        self._config.set_include_content(include_content)

        options = {
            "include_details": include_details,
            "include_content": include_content,
        }

        if self._on_confirm:
            self._on_confirm(selected, options)
        else:
            # Default: push progress screen
            from .progress_screen import ProgressScreen
            self.app.push_screen(ProgressScreen(selected, options))

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()

    def action_confirm(self) -> None:
        """Confirm selection."""
        self._confirm_selection()

    def action_select_all(self) -> None:
        """Select all object types."""
        selection_list = self.query_one("#selection-list", SelectionList)
        for key, _, _ in OBJECT_TYPES:
            selection_list.select(key)
        self._update_summary()

    def action_select_none(self) -> None:
        """Deselect all object types."""
        selection_list = self.query_one("#selection-list", SelectionList)
        for key, _, _ in OBJECT_TYPES:
            selection_list.deselect(key)
        self._update_summary()
