"""
Convert Markdown to styled DOCX (with math) and optional PDF.

Usage:
  python scripts/md_to_styled_docx_pdf.py \
    --input "prm_opt/Model Scope v2.md" \
    --reference "prm_opt/Model Scope.docx" \
    --docx "prm_opt/Model Scope v2.docx" \
    --pdf "prm_opt/Model Scope v2.pdf"

Notes:
- Uses pandoc via pypandoc_binary.
- Uses Word COM automation for DOCX -> PDF export on Windows.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pypandoc


def md_to_docx(input_md: Path, output_docx: Path, reference_docx: Path | None) -> None:
    output_docx.parent.mkdir(parents=True, exist_ok=True)

    extra_args = [
        "--standalone",
        "--from=markdown+tex_math_dollars+tex_math_single_backslash",
    ]
    if reference_docx is not None:
        extra_args.append(f"--reference-doc={reference_docx}")

    pypandoc.convert_file(
        str(input_md),
        to="docx",
        format="md",
        outputfile=str(output_docx),
        extra_args=extra_args,
    )


def docx_to_pdf_via_word(input_docx: Path, output_pdf: Path) -> None:
    import win32com.client  # pyright: ignore[reportMissingImports]

    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    word = win32com.client.Dispatch("Word.Application")
    word.Visible = False
    word.DisplayAlerts = 0

    # 17 = wdExportFormatPDF
    wd_export_format_pdf = 17

    doc = None
    try:
        doc = word.Documents.Open(str(input_docx.resolve()))
        doc.ExportAsFixedFormat(
            OutputFileName=str(output_pdf.resolve()),
            ExportFormat=wd_export_format_pdf,
            OpenAfterExport=False,
            OptimizeFor=0,
            Range=0,
            Item=0,
            IncludeDocProps=True,
            KeepIRM=True,
            CreateBookmarks=1,
            DocStructureTags=True,
            BitmapMissingFonts=True,
            UseISO19005_1=False,
        )
    finally:
        if doc is not None:
            doc.Close(False)
        word.Quit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert markdown to styled docx/pdf.")
    parser.add_argument("--input", required=True, help="Input markdown path")
    parser.add_argument("--docx", required=True, help="Output DOCX path")
    parser.add_argument("--reference", help="Reference DOCX for styles")
    parser.add_argument("--pdf", help="Output PDF path (optional)")
    args = parser.parse_args()

    input_md = Path(args.input)
    output_docx = Path(args.docx)
    reference_docx = Path(args.reference) if args.reference else None
    output_pdf = Path(args.pdf) if args.pdf else None

    if not input_md.exists():
        raise FileNotFoundError(f"Input markdown not found: {input_md}")

    if reference_docx is not None and not reference_docx.exists():
        raise FileNotFoundError(f"Reference docx not found: {reference_docx}")

    md_to_docx(input_md=input_md, output_docx=output_docx, reference_docx=reference_docx)
    print(f"DOCX created: {output_docx}")

    if output_pdf is not None:
        if os.name != "nt":
            raise RuntimeError("PDF export via Word COM is supported on Windows only.")
        docx_to_pdf_via_word(input_docx=output_docx, output_pdf=output_pdf)
        print(f"PDF created: {output_pdf}")


if __name__ == "__main__":
    main()
