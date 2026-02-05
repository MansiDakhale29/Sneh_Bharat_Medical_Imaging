# extractors/pdf_extractor.py

import os
import re
import tempfile
from datetime import datetime
from typing import Dict, Any

import PyPDF2
from pdf2image import convert_from_path
from PIL import Image

# OCR
try:
    from extractors.ocr_report_extractor import OCRReportExtractor
except ImportError:
    OCRReportExtractor = None


class MedicalPDFExtractor:
    """
    Extract structured information from medical PDF reports.

    Supports:
    - Text-based PDFs (PyPDF2)
    - Image-based / scanned PDFs (OCR via OCRReportExtractor)
    """

    def __init__(self, use_ocr: bool = True, ocr_threshold: int = 100):
        """
        Args:
            use_ocr: enable OCR fallback
            ocr_threshold: min text length to consider PDF as text-based
        """
        self.use_ocr = use_ocr
        self.ocr_threshold = ocr_threshold
        self.ocr_extractor = OCRReportExtractor() if use_ocr and OCRReportExtractor else None

    # ===================== MAIN =====================

    def extract(self, file_path: str) -> Dict[str, Any]:
        extracted_data = {
            "file_info": self._get_file_info(file_path),
            "patient_information": {},
            "study_information": {},
            "clinical_information": {},
            "measurements": {},
            "raw_text": "",
            "extraction_method": "text",
            "processing_info": {
                "extracted_at": datetime.now().isoformat()
            }
        }

        # 1️⃣ Try text extraction
        text = self._extract_text_from_pdf(file_path)

        # 2️⃣ Decide OCR or not
        if len(text.strip()) < self.ocr_threshold and self.use_ocr:
            extracted_data["extraction_method"] = "ocr"
            return self._extract_via_ocr(file_path)

        # 3️⃣ Parse text-based PDF
        extracted_data["raw_text"] = text
        extracted_data["patient_information"] = self._extract_patient_info(text)
        extracted_data["study_information"] = self._extract_report_info(text)
        extracted_data["clinical_information"] = self._extract_clinical_data(text)
        extracted_data["measurements"] = self._extract_measurements(text)

        return extracted_data

    # ===================== OCR PDF =====================

    def _extract_via_ocr(self, file_path: str, dpi: int = 300) -> Dict[str, Any]:
        if not self.ocr_extractor:
            return {
                "file_info": self._get_file_info(file_path),
                "error": "OCR extractor not available",
                "patient_information": {},
                "study_information": {},
                "clinical_information": {},
                "measurements": {},
                "raw_text": ""
            }

        images = convert_from_path(file_path, dpi=dpi)

        combined_text = ""
        final_result = {}

        for idx, image in enumerate(images):
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                image.save(tmp.name, "PNG")
                page_result = self.ocr_extractor.extract_from_image(tmp.name)

            os.remove(tmp.name)

            combined_text += f"\n--- Page {idx + 1} ---\n{page_result.get('raw_text', '')}\n"

            if idx == 0:
                final_result = page_result

        # 🔁 Normalize keys for DB compatibility
        normalized = {
            "file_info": self._get_file_info(file_path),
            "source_type": "ocr_pdf",
            "extraction_method": "ocr",
            "processing_info": {
                "extracted_at": datetime.now().isoformat(),
                "method": "pdf_to_image_ocr"
            },
            "raw_text": combined_text,
            "patient_information": final_result.get("patient_info", {}),
            "study_information": final_result.get("report_info", {}),
            "clinical_information": final_result.get("clinical_data", {}),
            "measurements": final_result.get("measurements", {})
        }

        return normalized

    # ===================== TEXT PDF =====================

    def _extract_text_from_pdf(self, file_path: str) -> str:
        text = ""
        try:
            with open(file_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print("PDF text extraction error:", e)
        return text

    # ===================== PARSERS =====================

    def _extract_patient_info(self, text: str) -> Dict[str, str]:
        info = {}

        patterns = {
            "patient_id": [
                r'Patient\s*[I1]D[:\s]+([\w-]+)',
                r'UHID[:\s]+([\w-]+)',
                r'Reg\.?\s*No\.?[:\s]+([\w-]+)'
            ],
            "patient_name": [
                r'Patient\s*(?:Name|Namo)[:\s]+([A-Z][A-Z\s]+)',
                r'(?:Name|Namo)[:\s]+([A-Z][A-Z\s]+)'
            ],
            "patient_age": [
                r'Age[:\s]+(\d+)',
                r'(\d+)\s*[Yy]'
            ],
            "patient_sex": [
                r'Sex[:\s]+(Male|Female|M|F)',
                r'\d+\s*[Yy]\s*(M|F)'
            ]
        }

        for key, plist in patterns.items():
            for p in plist:
                m = re.search(p, text, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if key == "patient_sex":
                        val = "M" if val.upper().startswith("M") else "F"
                    info[key] = val
                    break

        return info

    def _extract_report_info(self, text: str) -> Dict[str, str]:
        info = {}

        date_patterns = [
            r'(?:Report\s*Date|Date)[:\s]+(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})'
        ]

        for p in date_patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                info["report_date"] = m.group(1)
                break

        modalities = ["CT", "MRI", "X-RAY", "XRAY", "ULTRASOUND", "USG"]
        for mod in modalities:
            if mod in text.upper():
                info["modality"] = mod
                break

        return info

    def _extract_clinical_data(self, text: str) -> Dict[str, str]:
        data = {}

        sections = {
            "findings": r'(Findings|Observations?)[:\s]+(.+?)(?=Impression|Conclusion|Advice|$)',
            "impression": r'(Impression|Conclusion|Diagnosis)[:\s]+(.+?)(?=Advice|$)',
            "recommendations": r'(Advice|Recommendation)[:\s]+(.+?)$'
        }

        for key, pattern in sections.items():
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if m:
                data[key] = m.group(2).strip()

        return data

    def _extract_measurements(self, text: str) -> Dict[str, Any]:
        measurements = {}
        pattern = r'(\d+\.?\d*)\s*(mm|cm|ml|HU|mg|kg|L|cc)'
        for i, m in enumerate(re.finditer(pattern, text, re.IGNORECASE)):
            measurements[f"measurement_{i+1}"] = {
                "value": float(m.group(1)),
                "unit": m.group(2)
            }
        return measurements

    # ===================== UTILS =====================

    def _get_file_info(self, file_path: str) -> Dict[str, Any]:
        return {
            "filename": os.path.basename(file_path),
            "file_size_mb": round(os.path.getsize(file_path) / (1024 * 1024), 2),
            "format": os.path.splitext(file_path)[1]
        }
