"""Worker parse — link/title/text extraction."""

from __future__ import annotations

from crawler.parser import extract_links, extract_text, extract_title, parse_page

HTML = """<!DOCTYPE html>
<html><head><title>Hello World</title></head>
<body>
  <h1>Ignored when title present</h1>
  <script>var x=1;</script>
  <a href="/relative">Rel</a>
  <a href="https://example.com/abs">Abs</a>
  <a href="https://example.com/abs">Dup</a>
  <a href="mailto:a@b.c">Mail</a>
  <a href="#frag">Frag</a>
  <p>Some body text here.</p>
</body></html>
"""


def test_extract_title():
    assert extract_title(HTML) == "Hello World"


def test_extract_links_absolute_and_deduped():
    links = extract_links("https://example.com/base", HTML)
    assert "https://example.com/relative" in links
    assert "https://example.com/abs" in links
    assert links.count("https://example.com/abs") == 1
    assert not any(link.startswith("mailto:") for link in links)


def test_extract_text_strips_scripts():
    text = extract_text(HTML)
    assert "var x" not in text
    assert "Some body text here" in text


def test_parse_page_tuple():
    title, text, links = parse_page("https://example.com/", HTML)
    assert title == "Hello World"
    assert len(links) >= 2
    assert "body text" in text.lower()
