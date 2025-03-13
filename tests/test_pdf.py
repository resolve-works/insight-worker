import pytest
from unittest.mock import patch, MagicMock
from insight_worker.pdf import PdfService


@pytest.fixture
def pdf_service():
    return PdfService()


def test_get_pdf_page_count(pdf_service):
    with patch("insight_worker.pdf.Pdf") as mock_pdf:
        mock_pdf_instance = MagicMock()
        mock_pdf_instance.pages = [1, 2, 3]
        mock_pdf.open.return_value.__enter__.return_value = mock_pdf_instance

        count = pdf_service.get_pdf_page_count("test.pdf")
        assert count == 3
        mock_pdf.open.assert_called_once_with("test.pdf")


def test_repair_pdf(pdf_service):
    with patch("insight_worker.pdf.subprocess") as mock_subprocess:
        pdf_service.repair_pdf("input.pdf", "output.pdf")

        mock_subprocess.check_call.assert_called_once_with(
            [
                "/usr/bin/gs",
                "-dSAFER",
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                "-o",
                "output.pdf",
                "input.pdf",
            ]
        )


def test_extract_pdf_pages_text(pdf_service):
    with patch("insight_worker.pdf.extract_pages") as mock_extract_pages:
        # Create mock page layouts with text elements
        mock_text_element1 = MagicMock()
        mock_text_element1.get_text.return_value = "Text from page 1"

        mock_text_element2 = MagicMock()
        mock_text_element2.get_text.return_value = "Text from page 2"

        # Mock non-text element that should be skipped
        mock_non_text_element = MagicMock()

        # Set up the mock to return two page layouts
        mock_extract_pages.return_value = [
            [mock_text_element1, mock_non_text_element],
            [mock_text_element2],
        ]

        # Define the mock for isinstance to identify text containers
        with patch(
            "insight_worker.pdf.isinstance",
            lambda obj, cls: obj == mock_text_element1 or obj == mock_text_element2,
        ):
            # Call the method and check results
            texts = pdf_service.extract_pdf_pages_text("test.pdf")

            assert len(texts) == 2
            assert texts[0] == "Text from page 1"
            assert texts[1] == "Text from page 2"
            mock_extract_pages.assert_called_once_with("test.pdf")


def test_validate_pdf_mime_type(pdf_service):
    with patch("insight_worker.pdf.magic.from_file") as mock_magic:
        # Test PDF file
        mock_magic.return_value = "application/pdf"
        assert pdf_service.validate_pdf_mime_type("test.pdf") is True

        # Test non-PDF file
        mock_magic.return_value = "text/plain"
        assert pdf_service.validate_pdf_mime_type("test.txt") is False


def test_optimize_pdf(pdf_service):
    with patch("insight_worker.pdf.Process") as mock_process:
        mock_process_instance = MagicMock()
        mock_process.return_value = mock_process_instance

        pdf_service.optimize_pdf("input.pdf", "output.pdf")

        mock_process.assert_called_once()
        mock_process_instance.start.assert_called_once()
        mock_process_instance.join.assert_called_once()
