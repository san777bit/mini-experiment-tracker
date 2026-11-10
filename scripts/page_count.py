"""How many A4 pages is a Markdown note? (The task limits the note to 2 pages, the design to 1.)

Minimal md -> html (headings, paragraphs, tables, bold, code), printed by Chromium (Playwright) at
11 pt with 2 cm margins. An estimate: real renderers differ by a few lines.

    .venv/Scripts/python scripts/page_count.py NOTE.md DESIGN.md
"""
import asyncio
import html
import re
import sys

from playwright.async_api import async_playwright


def inline(t: str) -> str:
    t = html.escape(t)
    t = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", t)
    t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
    return re.sub(r"\[(.+?)\]\((.+?)\)", r"<a>\1</a>", t)


def md(src: str) -> str:
    out: list[str] = []
    para: list[str] = []
    table: list[str] = []

    def flush() -> None:
        if para:
            out.append("<p>" + inline(" ".join(para)) + "</p>")
            para.clear()
        if table:
            rows = [r for r in table if not re.match(r"^\|[-| ]+\|$", r)]
            head = "".join(f"<th>{inline(c.strip())}</th>" for c in rows[0].strip("|").split("|"))
            body = "".join("<tr>" + "".join(f"<td>{inline(c.strip())}</td>" for c in r.strip("|").split("|"))
                           + "</tr>" for r in rows[1:])
            out.append(f"<table><tr>{head}</tr>{body}</table>")
            table.clear()

    for line in src.splitlines():
        if line.startswith("|"):
            if para:
                flush()
            table.append(line)
        elif line.startswith("#"):
            flush()
            n = len(line) - len(line.lstrip("#"))
            out.append(f"<h{n}>{inline(line.lstrip('# '))}</h{n}>")
        elif line.startswith("```"):
            flush()
        elif not line.strip():
            flush()
        else:
            if table:
                flush()
            para.append(line)
    flush()
    return "\n".join(out)


async def main(paths: list[str]) -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page()
        for path in paths:
            doc = ("<html><head><meta charset='utf-8'><style>body{font:11pt Calibri,Arial,sans-serif;"
                   "line-height:1.3}table{border-collapse:collapse;font-size:10pt}td,th{border:1px solid #999;"
                   "padding:2px 4px}h1{font-size:16pt}h2{font-size:13pt;margin:10px 0 4px}p{margin:4px 0}"
                   "</style></head><body>" + md(open(path, encoding="utf-8").read()) + "</body></html>")
            await page.set_content(doc)
            pdf = await page.pdf(format="A4", margin={k: "2cm" for k in ("top", "bottom", "left", "right")})
            pages = len(re.findall(rb"/Type\s*/Page[^s]", pdf))
            print(f"{path}: {pages} pages")
        await browser.close()


asyncio.run(main(sys.argv[1:]))
