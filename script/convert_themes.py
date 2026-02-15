#!/usr/bin/env python3
"""
Convert ZQLZ theme JSON files to Zed's native theme format.

Usage: python scripts/convert_themes.py
"""

import json
import os
from pathlib import Path


def convert_syntax(old_syntax: dict) -> dict:
    """Convert ZQLZ syntax highlighting to Zed format."""
    syntax = {}

    for key, value in old_syntax.items():
        if isinstance(value, dict):
            color = value.get("color", "")
            font_style = value.get("font_style")
            font_weight = value.get("font_weight")

            entry = {"color": color}
            if font_style:
                entry["font_style"] = font_style
            if font_weight:
                entry["font_weight"] = font_weight
            syntax[key] = entry

    return syntax


def convert_theme(old_theme: dict) -> dict:
    """Convert a single ZQLZ theme to Zed format."""
    colors = old_theme.get("colors", {})
    highlight = old_theme.get("highlight", {})

    mode = old_theme.get("mode", "dark")
    appearance = "light" if mode == "light" else "dark"

    style = {
        "background": colors.get("background", "#1a1b26"),
        "text": colors.get("foreground", "#c0caf5"),
        "text.muted": colors.get("muted.foreground", "#565f89"),
        "text.accent": colors.get("foreground", "#c0caf5"),
        "text.placeholder": colors.get("muted.foreground", "#565f89"),
        "text.disabled": colors.get("muted.foreground", "#565f89"),
        "border": colors.get("border", "#292e42"),
        "border.variant": colors.get("border", "#292e42"),
        "border.focused": colors.get("primary.background", "#7aa2f7"),
        "border.selected": colors.get("primary.background", "#7aa2f7"),
        "border.disabled": colors.get("input.border", colors.get("border", "#292e42")),
        "border.transparent": None,
        "panel.background": colors.get(
            "panel.background", colors.get("muted.background", "#292e42")
        ),
        "panel.focused_border": colors.get("primary.background", "#7aa2f7"),
        "panel.indent_guide": colors.get("muted.background", "#292e42"),
        "panel.indent_guide_active": colors.get("primary.background", "#7aa2f7"),
        "elevated_surface.background": colors.get(
            "popover.background", colors.get("background", "#1a1b26")
        ),
        "surface.background": colors.get("muted.background", "#292e42"),
        "tab_bar.background": colors.get(
            "tab_bar.background", colors.get("title_bar.background", "#161720")
        ),
        "tab.active_background": colors.get(
            "tab.active.background", colors.get("background", "#1a1b26")
        ),
        "tab.inactive_background": colors.get(
            "secondary.background", colors.get("muted.background", "#292e42")
        ),
        "tab.text": colors.get(
            "tab.foreground", colors.get("muted.foreground", "#565f89")
        ),
        "tab.active_text": colors.get(
            "tab.active.foreground", colors.get("foreground", "#c0caf5")
        ),
        "title_bar.background": colors.get("title_bar.background", "#161720"),
        "title_bar.inactive_background": colors.get("title_bar.background", "#161720"),
        "toolbar.background": colors.get(
            "panel.background", colors.get("muted.background", "#292e42")
        ),
        "status_bar.background": colors.get("title_bar.background", "#161720"),
        "icon": colors.get("foreground", "#c0caf5"),
        "icon.muted": colors.get("muted.foreground", "#565f89"),
        "icon.accent": colors.get("primary.background", "#7aa2f7"),
        "icon.disabled": colors.get("muted.foreground", "#565f89"),
        "icon.placeholder": colors.get("muted.foreground", "#565f89"),
        "element.background": colors.get(
            "secondary.background", colors.get("muted.background", "#292e42")
        ),
        "element.hover": colors.get("secondary.hover.background", "#31374f"),
        "element.active": colors.get("primary.background", "#7aa2f7"),
        "element.selected": colors.get("list.active.background", "#7aa2f722"),
        "element.disabled": colors.get("muted.foreground", "#565f89"),
        "ghost_element.hover": colors.get("list.active.background", "#7aa2f711"),
        "ghost_element.active": colors.get("list.active.background", "#7aa2f722"),
        "ghost_element.selected": colors.get("list.active.background", "#7aa2f722"),
        "ghost_element.disabled": colors.get("muted.foreground", "#565f89"),
        "drop_target.background": colors.get("list.active.background", "#7aa2f722"),
        "link_text.hover": colors.get(
            "link.hover.foreground", colors.get("link.foreground", "#7aa2f7")
        ),
        "scrollbar.track.background": colors.get("scrollbar.background", "#1a1b2600"),
        "scrollbar.track.border": None,
        "scrollbar.thumb.background": colors.get(
            "scrollbar.thumb.background", "#414868"
        ),
        "scrollbar.thumb.border": None,
        "scrollbar.thumb.hover_background": colors.get("primary.background", "#7aa2f7"),
        "editor.background": highlight.get(
            "editor.background", colors.get("background", "#1a1b26")
        ),
        "editor.foreground": highlight.get(
            "editor.foreground", colors.get("foreground", "#c0caf5")
        ),
        "editor.gutter.background": highlight.get(
            "editor.background", colors.get("background", "#1a1b26")
        ),
        "editor.line_number": highlight.get("editor.line_number", "#565f89"),
        "editor.active_line_number": highlight.get(
            "editor.active_line_number", "#c0caf5"
        ),
        "editor.active_line.background": highlight.get(
            "editor.active_line.background", "#292e42"
        ),
        "editor.highlighted_line.background": highlight.get(
            "editor.active_line.background", "#292e42"
        ),
        "editor.indent_guide": colors.get("muted.background", "#292e42"),
        "editor.indent_guide_active": colors.get("primary.background", "#7aa2f7"),
        "editor.wrap_guide": colors.get("muted.background", "#292e42"),
        "editor.active_wrap_guide": colors.get("primary.background", "#7aa2f7"),
        "editor.invisible": colors.get("muted.foreground", "#565f89"),
        "editor.document_highlight.read_background": colors.get(
            "list.active.background", "#7aa2f711"
        ),
        "editor.document_highlight.write_background": colors.get(
            "list.active.background", "#7aa2f722"
        ),
        "editor.document_highlight.bracket_background": colors.get(
            "list.active.background", "#7aa2f722"
        ),
        "search.match_background": "#e0af6844",
        "conflict": highlight.get("conflict", "#f7768e"),
        "conflict.background": highlight.get(
            "conflict.background", f"{highlight.get('conflict', '#f7768e')}11"
        ),
        "conflict.border": highlight.get(
            "conflict.border", highlight.get("conflict", "#f7768e")
        ),
        "created": highlight.get("created", "#9ece6a"),
        "created.background": highlight.get(
            "created.background", f"{highlight.get('created', '#9ece6a')}11"
        ),
        "created.border": highlight.get(
            "created.border", highlight.get("created", "#9ece6a")
        ),
        "deleted": highlight.get("deleted", "#f7768e"),
        "deleted.background": highlight.get(
            "deleted.background", f"{highlight.get('deleted', '#f7768e')}11"
        ),
        "deleted.border": highlight.get(
            "deleted.border", highlight.get("deleted", "#f7768e")
        ),
        "error": highlight.get("error", "#f7768e"),
        "error.background": highlight.get(
            "error.background", f"{highlight.get('error', '#f7768e')}11"
        ),
        "error.border": highlight.get(
            "error.border", highlight.get("error", "#f7768e")
        ),
        "hidden": highlight.get("hidden", "#565f89"),
        "hidden.background": highlight.get("hidden.background"),
        "hidden.border": highlight.get(
            "hidden.border", highlight.get("hidden", "#565f89")
        ),
        "hint": highlight.get("hint", "#7dcfff"),
        "hint.background": highlight.get(
            "hint.background", f"{highlight.get('hint', '#7dcfff')}11"
        ),
        "hint.border": highlight.get("hint.border", highlight.get("hint", "#7dcfff")),
        "ignored": highlight.get("ignored", "#565f89"),
        "ignored.background": highlight.get("ignored.background"),
        "ignored.border": highlight.get(
            "ignored.border", highlight.get("ignored", "#565f89")
        ),
        "info": highlight.get("info", "#7aa2f7"),
        "info.background": highlight.get(
            "info.background", f"{highlight.get('info', '#7aa2f7')}11"
        ),
        "info.border": highlight.get("info.border", highlight.get("info", "#7aa2f7")),
        "modified": highlight.get("modified", "#e0af68"),
        "modified.background": highlight.get(
            "modified.background", f"{highlight.get('modified', '#e0af68')}11"
        ),
        "modified.border": highlight.get(
            "modified.border", highlight.get("modified", "#e0af68")
        ),
        "predictive": highlight.get("predictive", "#565f89"),
        "predictive.background": highlight.get("predictive.background"),
        "predictive.border": highlight.get(
            "predictive.border", highlight.get("predictive", "#565f89")
        ),
        "renamed": highlight.get("renamed", "#7aa2f7"),
        "renamed.background": highlight.get(
            "renamed.background", f"{highlight.get('renamed', '#7aa2f7')}11"
        ),
        "renamed.border": highlight.get(
            "renamed.border", highlight.get("renamed", "#7aa2f7")
        ),
        "success": highlight.get("success", "#9ece6a"),
        "success.background": highlight.get(
            "success.background", f"{highlight.get('success', '#9ece6a')}11"
        ),
        "success.border": highlight.get(
            "success.border", highlight.get("success", "#9ece6a")
        ),
        "unreachable": highlight.get("unreachable", "#565f89"),
        "unreachable.background": highlight.get("unreachable.background"),
        "unreachable.border": highlight.get(
            "unreachable.border", highlight.get("unreachable", "#565f89")
        ),
        "warning": highlight.get("warning", "#e0af68"),
        "warning.background": highlight.get(
            "warning.background", f"{highlight.get('warning', '#e0af68')}11"
        ),
        "warning.border": highlight.get(
            "warning.border", highlight.get("warning", "#e0af68")
        ),
        "syntax": convert_syntax(highlight.get("syntax", {})),
        "players": [
            {"cursor": "#7aa2f7", "background": "#7aa2f7", "selection": "#364A82"},
            {"cursor": "#9ece6a", "background": "#9ece6a", "selection": "#4A572A"},
            {"cursor": "#e0af68", "background": "#e0af68", "selection": "#774A1A"},
            {"cursor": "#f7768e", "background": "#f7768e", "selection": "#7F3A3A"},
            {"cursor": "#bb9af7", "background": "#bb9af7", "selection": "#5A4A7A"},
            {"cursor": "#7dcfff", "background": "#7dcfff", "selection": "#2A4A5A"},
        ],
    }

    return {
        "name": old_theme.get("name", "Unknown"),
        "appearance": appearance,
        "style": style,
    }


def convert_theme_file(input_path: Path, output_path: Path):
    """Convert a single theme file."""
    with open(input_path, "r") as f:
        old_data = json.load(f)

    new_data = {
        "$schema": "https://zed.dev/schema/themes/v0.2.0.json",
        "name": old_data.get("name", "Unknown"),
        "author": old_data.get("author", "Unknown"),
        "themes": [],
    }

    for old_theme in old_data.get("themes", []):
        new_data["themes"].append(convert_theme(old_theme))

    with open(output_path, "w") as f:
        json.dump(new_data, f, indent=2)

    print(f"Converted: {input_path.name} -> {output_path.name}")


def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    themes_dir = project_root / "crates" / "zqlz-app" / "assets" / "themes"

    already_converted = {
        "catppuccin.json",
        "gruvbox.json",
        "tokyonight.json",
        "solarized.json",
        "everforest.json",
    }

    for theme_file in themes_dir.glob("*.json"):
        if theme_file.name in already_converted:
            print(f"Skipping already converted: {theme_file.name}")
            continue

        convert_theme_file(theme_file, theme_file)


if __name__ == "__main__":
    main()
