import pydicom
from PIL import Image
import os
from datetime import datetime
from typing import Dict, Any

from extractors.pdf_extractor import MedicalPDFExtractor
from extractors.ocr_report_extractor import OCRReportExtractor


class MetadataExtractor:
    """
    Extract metadata from medical files (DICOM, Images, PDF)
    """

    def __init__(self):
        self.pdf_extractor = MedicalPDFExtractor()
        self.ocr_extractor = OCRReportExtractor()

    def extract(self, file_path: str) -> Dict[str, Any]:
        file_ext = os.path.splitext(file_path)[1].lower()

        try:
            if file_ext in ['.dcm', '.dicom', '.dic']:
                return self.extract_dicom(file_path)
            elif file_ext in ['.jpg', '.jpeg', '.png', '.tif', '.tiff']:
                return self.extract_medical_image(file_path)
            elif file_ext == '.pdf':
                return self.pdf_extractor.extract(file_path)
            else:
                return {'error': f'Unsupported file format: {file_ext}'}
        except Exception as e:
            return {'error': f'Extraction failed: {str(e)}'}

    # ===================== DICOM =====================

    def extract_dicom(self, file_path: str) -> Dict[str, Any]:
        ds = pydicom.dcmread(file_path)

        metadata = {
            "file_info": {
                "filename": os.path.basename(file_path),
                "file_size_mb": round(os.path.getsize(file_path) / (1024 * 1024), 2),
                "format": "DICOM"
            },
            "patient_information": {
                "patient_name": str(ds.get("PatientName", "N/A")),
                "patient_id":   str(ds.get("PatientID",   "N/A")),
                "patient_sex":  str(ds.get("PatientSex",  "N/A")),
                "patient_age":  str(ds.get("PatientAge",  "N/A")),
            },
            "study_information": {
                "study_instance_uid": str(ds.get("StudyInstanceUID", "N/A")),
                "study_date":         self._format_dicom_date(str(ds.get("StudyDate", ""))),
                "study_description":  str(ds.get("StudyDescription", "N/A")),
                "modality":           str(ds.get("Modality", "N/A")),
            },
            "image_information": {
                "rows":          int(ds.get("Rows", 0)),
                "columns":       int(ds.get("Columns", 0)),
                "window_center": str(ds.get("WindowCenter", "N/A")),
                "window_width":  str(ds.get("WindowWidth",  "N/A")),
            },
            "processing_info": {
                "extracted_at":     datetime.now().isoformat(),
                "extractor_version": "1.0"
            }
        }

        metadata["all_dicom_tags"] = self._get_all_tags(ds)
        return metadata

    # ===================== MEDICAL IMAGE (OCR) =====================

    def extract_medical_image(self, file_path: str) -> Dict[str, Any]:
        """
        Run OCR on the image.  Three outcomes:
          1. OCR finds a medical report  →  normalised result returned.
          2. OCR reads text but it is not a medical report  →  fallback
             returned WITH the raw_text so the developer can see exactly
             what Tesseract read and why nothing matched.
          3. OCR reads nothing at all  →  fallback, raw_text is empty.

        Bug that was here before:
          • RGBA PNGs were passed straight to cv2.imread which silently
            drops the alpha channel inconsistently across platforms.
            Now we force-convert to RGB before saving the temp copy that
            the OCR pipeline reads.
          • On fallback the raw_text was discarded entirely, making it
            impossible to distinguish case 2 from case 3.
        """
        try:
            # --- RGBA guard -----------------------------------------------
            # cv2.imread drops alpha unpredictably; convert once, up front.
            img = Image.open(file_path)
            if img.mode in ("RGBA", "LA", "P"):
                rgb_path = file_path + ".rgb.png"
                img.convert("RGB").save(rgb_path)
                ocr_input = rgb_path
            else:
                ocr_input = file_path
                rgb_path  = None                # nothing to clean up

            # --- run OCR ------------------------------------------------------
            ocr_result = self.ocr_extractor.extract_from_image(ocr_input)

            # clean up temp file
            if rgb_path and os.path.exists(rgb_path):
                os.remove(rgb_path)

            raw_text = ocr_result.get('raw_text', '')

            # --- log every time so the developer can see what happened --------
            print(f"[OCR] file={os.path.basename(file_path)}  "
                  f"text_len={len(raw_text)}  "
                  f"patient_info={ocr_result.get('patient_info', {})}")

            # --- route on result ----------------------------------------------
            if 'error' not in ocr_result and ocr_result.get('patient_info'):
                # Case 1 — medical report found
                return self._normalize_ocr_result(ocr_result)
            else:
                # Case 2 or 3 — not a medical report (or blank)
                print(f"[OCR] FALLBACK — no patient_info extracted.  "
                      f"First 200 chars of raw_text: {raw_text[:200]!r}")
                return self._fallback_with_context(file_path, raw_text)

        except Exception as e:
            print(f"[OCR] EXCEPTION: {e}")
            return self._fallback_with_context(file_path, "")

    # ===================== FALLBACK (replaces extract_basic_image) =====================

    def _fallback_with_context(self, file_path: str, raw_text: str) -> Dict[str, Any]:
        """
        Fallback that preserves everything the developer needs to
        understand *why* patient data is missing.
        """
        img = Image.open(file_path)

        return {
            "file_info": {
                "filename":    os.path.basename(file_path),
                "file_size_mb": round(os.path.getsize(file_path) / (1024 * 1024), 2),
                "format":      img.format
            },
            "image_properties": {
                "width":  img.width,
                "height": img.height,
                "mode":   img.mode
            },
            "patient_information": {
                "patient_name": "Unknown",
                "patient_id":   "UNKNOWN",
            },
            "processing_info": {
                "extracted_at":     datetime.now().isoformat(),
                "extraction_method": "basic_image_only",
                # ← these two fields are the whole point of this change
                "ocr_ran":          True,
                "ocr_raw_text":     raw_text,   # empty string if Tesseract read nothing
            }
        }

    # ===================== NORMALISE =====================

    def _normalize_ocr_result(self, ocr_result: Dict[str, Any]) -> Dict[str, Any]:
        patient_info  = ocr_result.get('patient_info', {})
        report_info   = ocr_result.get('report_info', {})
        clinical_data = ocr_result.get('clinical_data', {})

        return {
            "file_info": ocr_result.get('file_info', {}),

            "patient_information": {
                "patient_name": patient_info.get('patient_name', 'Unknown'),
                "patient_id":   patient_info.get('patient_id',   'UNKNOWN'),
                "patient_sex":  patient_info.get('patient_sex',  'N/A'),
                "patient_age":  patient_info.get('patient_age',  'N/A'),
            },

            "study_information": {
                "study_date":        report_info.get('report_date',  'N/A'),
                "study_description": report_info.get('study_type',   'N/A'),
                "modality":          report_info.get('modality',     'N/A'),
                "report_type":       report_info.get('report_type',  'N/A'),
                "examination":       report_info.get('examination',  'N/A'),
            },

            "clinical_information": {
                "protocol":       clinical_data.get('protocol',        'N/A'),
                "clinical_brief": clinical_data.get('clinical_brief',  'N/A'),
                "observations":   clinical_data.get('observations',    'N/A'),
                "impression":     clinical_data.get('impression',      'N/A'),
                "advice":         clinical_data.get('advice',          'N/A'),
            },

            "processing_info": {
                "extracted_at":     datetime.now().isoformat(),
                "extraction_method": "ocr",
                "text_length":      ocr_result.get('processing_info', {}).get('text_length', 0)
            },

            "raw_text":        ocr_result.get('raw_text', ''),
            "ocr_raw_result":  ocr_result
        }

    # ===================== HELPERS =====================

    def _get_all_tags(self, ds: pydicom.Dataset) -> Dict[str, str]:
        tags = {}
        for elem in ds:
            if elem.VR != "SQ":
                val = str(elem.value)
                tags[elem.name] = val[:200] + "..." if len(val) > 200 else val
        return tags

    def _format_dicom_date(self, date_str: str) -> str:
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        return date_str