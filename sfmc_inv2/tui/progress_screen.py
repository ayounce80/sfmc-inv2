"""Progress screen for displaying extraction progress.

Shows per-extractor progress bars, item counts, and status updates
using Textual widgets.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    ProgressBar,
    Static,
)

from ..orchestration import ExtractorRunner, RunnerConfig, RunnerResult
from ..output import SnapshotWriter


class ProgressScreen(Screen):
    """Screen for displaying extraction progress."""

    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    CSS = """
    ProgressScreen {
        background: $surface;
    }

    #main-container {
        width: 100%;
        height: 100%;
        padding: 1 2;
    }

    #status-label {
        height: 3;
        content-align: center middle;
        text-style: bold;
    }

    #progress-table {
        height: 1fr;
        margin: 1 0;
    }

    #overall-progress {
        height: 3;
        padding: 0 1;
    }

    #elapsed-time {
        height: 1;
        text-align: right;
    }

    #button-bar {
        height: 3;
        align: center middle;
    }

    #summary-container {
        height: auto;
        padding: 1;
        border: solid $primary;
        margin: 1 0;
    }
    """

    def __init__(
        self,
        extractors: list[str],
        options: dict[str, Any],
        output_dir: Optional[str] = None,
        **kwargs: Any,
    ):
        """Initialize the progress screen.

        Args:
            extractors: List of extractor names to run.
            options: Extraction options.
            output_dir: Output directory path.
        """
        super().__init__(**kwargs)
        self._extractors = extractors
        self._options = options
        self._output_dir = output_dir or "./inventory"

        self._start_time: Optional[datetime] = None
        self._result: Optional[RunnerResult] = None
        self._is_running = False
        self._extractor_status: dict[str, dict[str, Any]] = {}

        # Initialize status for each extractor
        for name in extractors:
            self._extractor_status[name] = {
                "status": "Pending",
                "items": 0,
                "stage": "",
            }

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()

        with Container(id="main-container"):
            yield Static("Extraction in Progress...", id="status-label")

            # Progress table
            table = DataTable(id="progress-table")
            table.add_columns("Extractor", "Status", "Items", "Stage")
            yield table

            # Overall progress
            with Horizontal(id="overall-progress"):
                yield Label("Overall: ")
                yield ProgressBar(total=len(self._extractors), id="overall-bar")

            yield Static("Elapsed: 0:00", id="elapsed-time")

            # Summary (shown after completion)
            with Container(id="summary-container"):
                yield Static("", id="summary-text")

            # Buttons
            with Horizontal(id="button-bar"):
                yield Button("View Output", id="btn-view", disabled=True)
                yield Button("New Extraction", id="btn-new", disabled=True)
                yield Button("Exit", id="btn-exit")

        yield Footer()

    def on_mount(self) -> None:
        """Called when screen is mounted."""
        self._init_table()
        self._start_extraction()

    def _init_table(self) -> None:
        """Initialize the progress table."""
        table = self.query_one("#progress-table", DataTable)

        for name in self._extractors:
            table.add_row(
                name.replace("_", " ").title(),
                "Pending",
                "0",
                "",
                key=name,
            )

    def _start_extraction(self) -> None:
        """Start the extraction process."""
        self._is_running = True
        self._start_time = datetime.now()

        # Start timer update
        self.set_interval(1, self._update_elapsed)

        # Run extraction in background
        asyncio.create_task(self._run_extraction())

    async def _run_extraction(self) -> None:
        """Run the extraction asynchronously."""
        try:
            # Configure runner with progress callback
            config = RunnerConfig(
                include_details=self._options.get("include_details", True),
                include_content=self._options.get("include_content", False),
                progress_callback=self._on_progress,
            )

            runner = ExtractorRunner(config)

            # Run extractors
            self._result = await runner.run(self._extractors)

            # Write output
            await self._write_output()

            # Update UI
            self._on_complete()

        except Exception as e:
            self._on_error(str(e))

    def _on_progress(
        self, extractor: str, current: int, total: int, stage: str
    ) -> None:
        """Handle progress updates from runner."""
        if extractor in self._extractor_status:
            self._extractor_status[extractor].update({
                "status": "Running" if "Completed" not in stage else "Completed",
                "items": current if current > 0 else self._extractor_status[extractor]["items"],
                "stage": stage,
            })

            # Update table on main thread
            self.call_from_thread(self._update_table_row, extractor)

    def _update_table_row(self, extractor: str) -> None:
        """Update a single row in the progress table."""
        table = self.query_one("#progress-table", DataTable)
        status = self._extractor_status[extractor]

        try:
            table.update_cell(extractor, "Status", status["status"])
            table.update_cell(extractor, "Items", str(status["items"]))
            table.update_cell(extractor, "Stage", status["stage"])
        except Exception:
            pass  # Row might not exist yet

    def _update_elapsed(self) -> None:
        """Update elapsed time display."""
        if self._start_time:
            elapsed = datetime.now() - self._start_time
            minutes = int(elapsed.total_seconds() // 60)
            seconds = int(elapsed.total_seconds() % 60)
            self.query_one("#elapsed-time", Static).update(
                f"Elapsed: {minutes}:{seconds:02d}"
            )

    async def _write_output(self) -> None:
        """Write extraction results to disk."""
        if not self._result:
            return

        writer = SnapshotWriter(Path(self._output_dir))
        await writer.write(self._result)

    def _on_complete(self) -> None:
        """Handle extraction completion."""
        self._is_running = False

        # Update status
        status_label = self.query_one("#status-label", Static)
        if self._result and self._result.success:
            status_label.update("Extraction Complete!")
        elif self._result and self._result.partial_success:
            status_label.update("Extraction Completed with Errors")
        else:
            status_label.update("Extraction Failed")

        # Update overall progress
        progress_bar = self.query_one("#overall-bar", ProgressBar)
        progress_bar.progress = len(self._extractors)

        # Update summary
        if self._result:
            stats = self._result.get_statistics()
            summary = f"""
Total Objects: {stats.total_objects}
Relationships: {stats.total_relationships}
Duration: {stats.total_duration_seconds:.1f}s
Extractors: {stats.extractors_succeeded}/{stats.extractors_run} succeeded
Output: {self._output_dir}
            """.strip()
            self.query_one("#summary-text", Static).update(summary)

        # Enable buttons
        self.query_one("#btn-view", Button).disabled = False
        self.query_one("#btn-new", Button).disabled = False

    def _on_error(self, message: str) -> None:
        """Handle extraction error."""
        self._is_running = False

        status_label = self.query_one("#status-label", Static)
        status_label.update(f"Error: {message}")

        self.query_one("#btn-new", Button).disabled = False

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        button_id = event.button.id

        if button_id == "btn-exit":
            self.app.exit()
        elif button_id == "btn-new":
            from .selection_screen import SelectionScreen
            self.app.switch_screen(SelectionScreen())
        elif button_id == "btn-view":
            # Open output directory
            import subprocess
            import sys

            output_path = Path(self._output_dir)
            if output_path.exists():
                if sys.platform == "darwin":
                    subprocess.run(["open", str(output_path)])
                elif sys.platform == "linux":
                    subprocess.run(["xdg-open", str(output_path)])
                else:
                    subprocess.run(["explorer", str(output_path)])

    def action_quit(self) -> None:
        """Quit the application."""
        if self._is_running:
            self.notify("Extraction in progress...", severity="warning")
        else:
            self.app.exit()
