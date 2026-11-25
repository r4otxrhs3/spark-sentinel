"""Render docs/images/demo.gif: a terminal session running the real `sentinel` commands.

The output in the GIF is captured from actual runs on the gallery fixtures, so regenerate it
whenever the report format changes:

    pip install pillow
    python scripts/make_demo_gif.py

Only the `pip install` line is typed, not run (it would need the network).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images" / "demo.gif"
FIXTURES = ROOT / "tests" / "fixtures" / "eventlogs" / "gallery"

COLS, ROWS = 100, 30
FONT_SIZE, PAD = 15, 16
FONT_DIRS = ["/usr/share/fonts/truetype/dejavu", "/Library/Fonts", "C:/Windows/Fonts"]

BG, FG, DIM, PROMPT = "#10161C", "#D5DDE2", "#7C8A93", "#7BC77E"
ANSI = {"31": "#F0675D", "32": "#7BC77E", "33": "#E8B54A", "36": "#5DB8C4", "37": "#D5DDE2"}
SGR = re.compile(r"\x1b\[([\d;]*)m")
OSC8 = re.compile(r"\x1b\]8;;[^\x1b]*\x1b\\")  # terminal hyperlinks: keep the text only


def font(bold: bool = False) -> ImageFont.FreeTypeFont:
    name = "DejaVuSansMono-Bold.ttf" if bold else "DejaVuSansMono.ttf"
    for folder in FONT_DIRS:
        if (Path(folder) / name).exists():
            return ImageFont.truetype(str(Path(folder) / name), FONT_SIZE)
    sys.exit(f"Font {name} not found; install DejaVu fonts")


REGULAR, BOLD = font(), font(bold=True)
CHAR_W = REGULAR.getlength("M")
LINE_H = int(FONT_SIZE * 1.45)
SIZE = (int(PAD * 2 + COLS * CHAR_W), PAD * 2 + ROWS * LINE_H)


def parse_ansi(line: str) -> list[tuple[str, str, bool]]:
    """Split a line into (text, colour, bold) runs."""
    line = OSC8.sub("", line)
    runs, color, bold, pos = [], FG, False, 0
    for match in SGR.finditer(line):
        if match.start() > pos:
            runs.append((line[pos : match.start()], color, bold))
        for code in (match.group(1) or "0").split(";"):
            if code in ("0", ""):
                color, bold = FG, False
            elif code == "1":
                bold = True
            elif code == "2":
                color = DIM
            elif code in ANSI:
                color = ANSI[code]
        pos = match.end()
    runs.append((line[pos:], color, bold))
    return [r for r in runs if r[0]]


def wrap(runs: list[tuple[str, str, bool]]) -> list[list[tuple[str, str, bool]]]:
    """Wrap a styled line at COLS characters, like a terminal does."""
    lines, current, width = [], [], 0
    for text, color, bold in runs:
        while text:
            room = COLS - width
            piece, text = text[:room], text[room:]
            current.append((piece, color, bold))
            width += len(piece)
            if width >= COLS:
                lines.append(current)
                current, width = [], 0
    if current or not lines:  # a line that filled the width exactly has no remainder
        lines.append(current)
    return lines


def draw(screen: list[list[tuple[str, str, bool]]], cursor: bool) -> Image.Image:
    image = Image.new("RGB", SIZE, BG)
    canvas = ImageDraw.Draw(image)
    visible = screen[-ROWS:]
    for row, runs in enumerate(visible):
        x, y = PAD, PAD + row * LINE_H
        for text, color, bold in runs:
            canvas.text((x, y), text, font=BOLD if bold else REGULAR, fill=color)
            x += len(text) * CHAR_W
        if cursor and row == len(visible) - 1:
            canvas.rectangle([x + 1, y + 2, x + CHAR_W - 1, y + FONT_SIZE + 2], fill=FG)
    return image


def run(args: list[str], cwd: Path) -> list[str]:
    env = {**os.environ, "COLUMNS": str(COLS), "NO_COLOR": ""}
    proc = subprocess.run(
        [sys.executable, "-m", "spark_sentinel", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return (proc.stderr + proc.stdout).rstrip("\n").splitlines()


def _theme_palette() -> Image.Image:
    """A palette of the theme's colours and their blends with the background.

    Building it from a frame instead lets rare colours (a yellow severity label) get merged
    into grey. Blends cover the anti-aliased edges of the glyphs.
    """
    colors = [BG, FG, DIM, PROMPT, *ANSI.values()]
    bg = Image.new("RGB", (1, 1), BG).getpixel((0, 0))
    swatch = []
    for color in colors:
        rgb = Image.new("RGB", (1, 1), color).getpixel((0, 0))
        for step in range(1, 6):  # 6 shades from background to full colour
            t = step / 5
            swatch.append(tuple(round(b + (c - b) * t) for b, c in zip(bg, rgb, strict=True)))
    image = Image.new("RGB", (len(swatch) + 1, 1), BG)
    for i, rgb in enumerate(swatch, start=1):
        image.putpixel((i, 0), rgb)
    return image.quantize(colors=len(swatch) + 1, method=Image.Quantize.MEDIANCUT)


def main() -> None:
    work = Path(tempfile.mkdtemp())
    logs = work / "eventlogs"
    logs.mkdir()
    for job in FIXTURES.iterdir():
        (logs / job.name).symlink_to(job, target_is_directory=True)

    scenes = [
        ("pip install spark-sentinel", ["Successfully installed spark-sentinel-0.1.0"]),
        (
            "sentinel analyze eventlogs/skew --profile laptop --select SKEW",
            run(
                ["analyze", "eventlogs/skew", "--profile", "laptop", "--select", "SKEW", "--color"],
                work,
            ),
        ),
        (
            "sentinel analyze eventlogs --profile laptop -f html -o report.html",
            run(
                ["analyze", "eventlogs", "--profile", "laptop", "-f", "html", "-o", "report.html"],
                work,
            ),
        ),
    ]

    frames: list[Image.Image] = []
    durations: list[int] = []
    screen: list[list[tuple[str, str, bool]]] = []

    def frame(ms: int, cursor: bool = False) -> None:
        frames.append(draw(screen, cursor))
        durations.append(ms)

    for command, output in scenes:
        prompt = [("$ ", PROMPT, True)]
        screen.append(prompt)
        frame(500, cursor=True)
        for i in range(3, len(command) + 3, 3):  # type three characters per frame
            screen[-1] = [*prompt, (command[:i], FG, False)]
            frame(45, cursor=True)
        frame(400)
        for line in output:
            screen.extend(wrap(parse_ansi(line)))
            frame(55)
        frame(2600)
        screen.append([])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    palette = _theme_palette()
    quantized = [f.quantize(palette=palette, dither=Image.Dither.NONE) for f in frames]
    quantized[0].save(
        OUT,
        save_all=True,
        append_images=quantized[1:],
        duration=durations,
        loop=0,
        optimize=True,
        disposal=1,
    )
    print(f"{OUT.relative_to(ROOT)}: {len(frames)} frames, {OUT.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
