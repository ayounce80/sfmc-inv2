"""Main Textual application for SFMC Inventory Tool.

Provides the entry point and screen management for the TUI.
"""

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer

from .selection_screen import SelectionScreen
from .progress_screen import ProgressScreen


class InventoryApp(App):
    """SFMC Inventory Tool TUI Application."""

    TITLE = "SFMC Inventory Tool"
    SUB_TITLE = "Extract and catalog SFMC objects"

    CSS = """
    Screen {
        background: $surface;
    }
    """

    SCREENS = {
        "selection": SelectionScreen,
        "progress": ProgressScreen,
    }

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("d", "toggle_dark", "Toggle Dark Mode"),
    ]

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        # Push the selection screen
        self.push_screen(SelectionScreen())

    def action_quit(self) -> None:
        """Quit the application."""
        self.exit()

    def action_toggle_dark(self) -> None:
        """Toggle dark mode."""
        self.dark = not self.dark


def run_tui() -> None:
    """Run the TUI application."""
    app = InventoryApp()
    app.run()


if __name__ == "__main__":
    run_tui()
