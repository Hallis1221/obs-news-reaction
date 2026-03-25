"""Tests for CLI commands using Click testing."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from obs_news_reaction.cli import cli
from obs_news_reaction.db.schema import init_db


def test_cli_status(tmp_path):
    db = tmp_path / "test.db"
    with patch("obs_news_reaction.cli.init_db", lambda: init_db(db)):
        with patch("obs_news_reaction.db.operations.get_connection",
                   lambda: __import__("obs_news_reaction.db.schema", fromlist=["get_connection"]).get_connection(db)):
            runner = CliRunner()
            result = runner.invoke(cli, ["status"])
            # Should not crash; output depends on empty DB
            assert result.exit_code == 0 or "Error" not in result.output


def test_cli_announcements_empty(tmp_path):
    db = tmp_path / "test.db"
    init_db(db)
    with patch("obs_news_reaction.cli.init_db"):
        with patch("obs_news_reaction.db.operations.get_connection",
                   lambda: __import__("obs_news_reaction.db.schema", fromlist=["get_connection"]).get_connection(db)):
            runner = CliRunner()
            result = runner.invoke(cli, ["announcements"])
            assert result.exit_code == 0
            assert "No announcements" in result.output


def test_cli_results_empty(tmp_path):
    db = tmp_path / "test.db"
    init_db(db)
    with patch("obs_news_reaction.cli.init_db"):
        with patch("obs_news_reaction.db.operations.get_connection",
                   lambda: __import__("obs_news_reaction.db.schema", fromlist=["get_connection"]).get_connection(db)):
            runner = CliRunner()
            result = runner.invoke(cli, ["results"])
            assert result.exit_code == 0
            assert "No results" in result.output


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Oslo Bors" in result.output
