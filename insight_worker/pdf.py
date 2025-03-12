import ocrmypdf
import magic
import subprocess
from multiprocessing import Process
from pikepdf import Pdf
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from typing import List


class PdfService:
    """Service for handling PDF processing operations"""

    def __init__(self):
        pass

    def repair_pdf(self, input_file: str, output_file: str) -> None:
        """Repair a potentially corrupt PDF file using Ghostscript

        Args:
            input_file: Path to the input PDF file
            output_file: Path to write the repaired PDF file
        """
        subprocess.check_call(
            [
                "/usr/bin/gs",
                "-dSAFER",
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                "-o",
                output_file,
                input_file,
            ]
        )

    def _ocrmypdf_process(self, input_file: str, output_file: str) -> None:
        """Internal OCR process that runs OCRmyPDF on a file

        Args:
            input_file: Path to the input PDF file
            output_file: Path to write the OCRed PDF file
        """
        ocrmypdf.ocr(
            input_file,
            output_file,
            # Default is pdf-a, but the PDF/a spec for some reason does not support
            # pdf alignment. PDFJS refuses to use range-requests on a pdf/a.
            output_type="pdf",
            color_conversion_strategy="RGB",
            progress_bar=False,
            # https://github.com/ocrmypdf/OCRmyPDF/issues/1162
            continue_on_soft_render_error=True,
            # Only use one thread
            jobs=1,
            # Skip pages with text layer on it
            # TODO - Enable user to force OCR
            skip_text=True,
            # plugins=["insight_worker.plugin"],
            # Lossless optimization
            optimize=2,
            invalidate_digital_signatures=True,
        )

    def optimize_pdf(self, input_path: str, output_path: str) -> None:
        """OCR and optimize a PDF file

        Runs OCRmyPDF in a separate process to avoid any memory leaks.

        Args:
            input_path: Path to the input PDF file
            output_path: Path to write the optimized PDF file
        """
        process = Process(target=self._ocrmypdf_process, args=(input_path, output_path))
        process.start()
        process.join()

    def extract_pdf_pages_text(self, path: str) -> List[str]:
        """Extract text from all pages in a PDF file

        Args:
            path: Path to the PDF file

        Returns:
            List of text content for each page
        """
        texts = []
        for page_layout in extract_pages(path):
            text = ""
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    text += element.get_text()

            texts.append(text)

        return texts

    def validate_pdf_mime_type(self, file_path: str) -> bool:
        """Check if the file is actually a PDF by MIME type

        Args:
            file_path: Path to the file to check

        Returns:
            True if the file is a PDF, False otherwise
        """
        mime = magic.from_file(file_path, mime=True)
        return mime == "application/pdf"

    def get_pdf_page_count(self, pdf_path: str) -> int:
        """Get the number of pages in a PDF file

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Number of pages in the PDF
        """
        with Pdf.open(pdf_path) as pdf:
            return len(pdf.pages)
