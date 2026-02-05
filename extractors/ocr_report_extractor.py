# extractors/ocr_report_extractor.py

import os
import re
import cv2
import numpy as np
from PIL import Image
import pytesseract
from datetime import datetime
from typing import Dict, Any, Optional



class OCRReportExtractor:
    """
    OCR-based extractor for medical report images
    Handles OCR errors like 1D instead of ID, 0 instead of O, etc.
    """

    def extract_from_image(self, file_path: str) -> Dict[str, Any]:
        """
        Extract structured data from a report image using OCR
        """
        try:
            img = Image.open(file_path)
            img_cv = cv2.imread(file_path)

            if img_cv is None:
                return {
                    "file_info": self._get_file_info(file_path),
                    "error": "Failed to load image"
                }

            text = self._run_ocr(img, img_cv)

            if not text.strip():
                return {
                    "file_info": self._get_file_info(file_path),
                    "error": "OCR failed or no readable text found"
                }

            return {
                "file_info": self._get_file_info(file_path),
                "source_type": "ocr_report_image",
                "processing_info": {
                    "extracted_at": datetime.now().isoformat(),
                    "method": "tesseract_ocr",
                    "text_length": len(text)
                },
                "raw_text": text,
                "patient_info": self._extract_patient_info(text),
                "report_info": self._extract_report_info(text),
                "clinical_data": self._extract_clinical_data(text),
                "measurements": self._extract_measurements(text)
            }
        except Exception as e:
            return {
                "file_info": self._get_file_info(file_path),
                "error": f"Extraction failed: {str(e)}"
            }

    # ================= OCR =================

    def _run_ocr(self, img: Image.Image, img_cv) -> str:
        """
        OCR with improved preprocessing - tries multiple methods
        """
        results = []
        
        try:
            # Method 1: Direct OCR
            try:
                text = pytesseract.image_to_string(img, config='--psm 6')
                results.append(text)
            except:
                pass
            
            # Convert to grayscale for preprocessing
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            
            # Method 2: Upscaled + Otsu (usually best for scanned documents)
            try:
                scale_percent = 200
                width = int(img_cv.shape[1] * scale_percent / 100)
                height = int(img_cv.shape[0] * scale_percent / 100)
                upscaled = cv2.resize(img_cv, (width, height), interpolation=cv2.INTER_CUBIC)
                gray_up = cv2.cvtColor(upscaled, cv2.COLOR_BGR2GRAY)
                _, otsu_up = cv2.threshold(gray_up, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                text = pytesseract.image_to_string(otsu_up, config='--psm 6')
                results.append(text)
            except:
                pass
            
            # Method 3: Denoising + Otsu
            try:
                denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
                _, otsu = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                text = pytesseract.image_to_string(otsu, config='--psm 6')
                results.append(text)
            except:
                pass
            
            # Return the longest result (usually most complete)
            if results:
                return max(results, key=len)
            
            return pytesseract.image_to_string(img)
            
        except Exception as e:
            try:
                return pytesseract.image_to_string(img)
            except:
                return ""

    # ================= PARSERS =================

    def _extract_patient_info(self, text: str) -> Dict[str, str]:
        """
        Extract patient information
        Handles OCR errors: 1D→ID, 0PD→OPD, Namo→Name, etc.
        """
        info = {}

        # Patient ID - handles OCR errors like "1D" instead of "ID" and "0PD" instead of "OPD"
        pid_patterns = [
            r'Patient\s*[I1]D\s*:\s*0?PD(\d+)',  # Handles both ID/1D and OPD/0PD
            r'Patient\s*[I1]D\s*:\s*([\w-]+)',
            r'UHID\s*:\s*([\w-]+)',
            r'Reg\.?\s*No\.?\s*:\s*([\w-]+)',
            r'[I1]D\s*:\s*0?PD(\d+)',
        ]
        for pattern in pid_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                pid = match.group(1)
                # Clean up underscores and other artifacts
                pid = pid.replace('_', '').strip()
                # If it looks like it should have OPD prefix, add it
                if pid.isdigit() and len(pid) > 6:
                    info["patient_id"] = "OPD" + pid
                else:
                    info["patient_id"] = pid
                break

        # Patient Name - handles OCR errors like "Namo" instead of "Name"
        # Also handles table format with brackets and pipes
        # Note: ¥ and µ can be OCR errors for Y
        name_patterns = [
            r'\[\s*Pat(?:i|l)ont\s*(?:Name|Namo)\s*:\s*([A-Z][A-Z\s]+?)(?:\s+\d+\s*[YMmyµ¥]|Age|\||$)',
            r'Patient\s*(?:Name|Namo)\s*:\s*([A-Z][A-Z\s]+?)(?:\s+\d+\s*[YMmyµ¥]|Age|\||$)',
            r'\|\s*Pat(?:i|l)ont\s*(?:Name|Namo)\s*:\s*([A-Z][A-Z\s]+?)(?:\s+\d+\s*[YMmyµ¥]|Age|\||$)',
            r'(?:Name|Namo)\s*:\s*([A-Z][A-Z\s]+?)(?:\s+\d+\s*[YMmyµ¥]|Age|\||$)',
        ]
        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Clean up the name
                name = match.group(1).strip()
                # Remove any trailing numbers, special chars, or year markers
                name = re.sub(r'\s*\d+.*$', '', name).strip()
                name = re.sub(r'[_\|\[\]]', '', name).strip()
                if len(name) > 2:  # Sanity check
                    info["patient_name"] = name
                    break

        # Age - extract from formats like "44YF" or "Age: 44"
        # Handles OCR errors where Y becomes ¥ or µ
        age_patterns = [
            r'(\d+)\s*[YMmyµ¥][MFmf]?',  # 44YF, 44¥F, 44Y format
            r'Age\s*:\s*(\d+)',
        ]
        for pattern in age_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info["patient_age"] = match.group(1)
                break

        # Sex - extract from formats like "44YF" or "Sex: F"
        # Handles OCR errors where Y becomes ¥ or µ
        sex_patterns = [
            r'\d+\s*[YMmyµ¥]\s*([MFmf])',  # From 44YF or 44¥F format
            r'Sex\s*:\s*([MFmf])',
            r'Sex\s*([MFmf])',
            r'Sex\s*:\s*(Male|Female)',
        ]
        for pattern in sex_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                sex_value = match.group(1).upper()
                info["patient_sex"] = 'M' if sex_value.startswith('M') else 'F'
                break

        # Accession Number
        acc_patterns = [
            r'Acc(?:e|o)ssion\s*Number\s*:\s*([\w-]+)',  # Handles "Accossion"
            r'Accession\s*No\.?\s*:\s*([\w-]+)',
        ]
        for pattern in acc_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info["accession_number"] = match.group(1).strip()
                break

        return info

    def _extract_report_info(self, text: str) -> Dict[str, str]:
        """
        Extract report metadata
        """
        info = {}

        # Study Date - handles formats like "22-Nov-2025" or "22/11/2025"
        date_patterns = [
            r'Study\s*(?:Date|Dato)\s*:\s*(\d{1,2}[-/]\w{3,}[-/]\d{2,4})',  # Handles "Dato"
            r'Date\s*:\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info["report_date"] = match.group(1)
                break

        # Study Type
        study_patterns = [
            r'Study\s*:\s*([A-Z]+)',
            r'Exam\s*:\s*([A-Z]+)',
        ]
        for pattern in study_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info["study_type"] = match.group(1).upper()
                break

        # Modality - handles OCR errors like "Madallty"
        modality_patterns = [
            r'(?:Modality|Madallty|Modallty)\s*:\s*([A-Z]+)',
        ]
        for pattern in modality_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info["modality"] = match.group(1).upper()
                break

        # Report type from content
        report_types = {
            "X-RAY": r'X[-\s]?RAY|XRAY',
            "MRI": r'MRI|MAGNETIC\s+RESONANCE',
            "CT": r'CT\s+(?:SCAN)?|COMPUTED\s+TOMOGRAPHY',
            "ULTRASOUND": r'ULTRASOUND|USG|SONOGRAPHY',
            "ECG": r'ECG|EKG|ELECTROCARDIOGRAM',
        }
        
        for report_type, pattern in report_types.items():
            if re.search(pattern, text, re.IGNORECASE):
                info["report_type"] = report_type
                break

        # Specific exam description
        exam_match = re.search(r'(X[-\s]?RAY|MRI|CT|USG)\s+([\w\s]+?)(?:PA|AP|LAT|VIEW|\n)', 
                              text, re.IGNORECASE)
        if exam_match:
            info["examination"] = exam_match.group(0).strip()

        # Referring Physician - handles OCR errors
        ref_doc_patterns = [
            r'(?:Referring|Roferring|Refering)\s*Physician\s*:\s*([A-Za-z\s.]+?)(?:\||Study|\n|$)',
        ]
        for pattern in ref_doc_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                doc_name = match.group(1).strip()
                if len(doc_name) > 2:
                    info["referring_physician"] = doc_name
                    break

        return info

    def _extract_clinical_data(self, text: str) -> Dict[str, str]:
        """
        Extract clinical findings, impressions, and other report sections
        """
        data = {}

        # Protocol
        protocol = re.search(r'PROTOCOL\s*:[-\s]*(.+?)(?=CLINICAL|OBSERVATIONS|FINDINGS|$)', 
                           text, re.IGNORECASE | re.DOTALL)
        if protocol:
            protocol_text = protocol.group(1).strip()
            # Clean special chars from OCR errors
            protocol_text = re.sub(r'[§¶]', '', protocol_text)
            data["protocol"] = protocol_text

        # Clinical Brief/History
        clinical = re.search(r'CLINICAL\s*(?:BRIEF|HISTORY|ABRIEF|ASRIEF)\s*:[-\s]*(.+?)(?=OBSERVATIONS|FINDINGS|IMPRESSION|$)', 
                            text, re.IGNORECASE | re.DOTALL)
        if clinical:
            data["clinical_brief"] = clinical.group(1).strip()

        # Observations/Findings
        observations = re.search(r'(?:OBSERVATIONS|ONSERVATIONS|FINDINGS)\s*:(.+?)(?=IMPRESSION|ADVICE|CONCLUSION|Disclaimer|$)', 
                                text, re.IGNORECASE | re.DOTALL)
        if observations:
            obs_text = observations.group(1).strip()
            # Clean up artifacts
            obs_text = re.sub(r'^\s*[\'"\']', '', obs_text)
            obs_text = re.sub(r'^The\s+', '', obs_text, flags=re.MULTILINE)  # Remove repeated "The"
            data["observations"] = obs_text

        # Impression/Conclusion
        impression = re.search(r'IMPRESSION\s*:[-\s]*(.+?)(?=ADVICE|CONCLUSION|Disclaimer|$)', 
                              text, re.IGNORECASE | re.DOTALL)
        if impression:
            data["impression"] = impression.group(1).strip()

        # Advice/Recommendations
        advice = re.search(r'ADVICE\s*:[-\s]*(.+?)(?=Disclaimer|NOTE|$)', 
                          text, re.IGNORECASE | re.DOTALL)
        if advice:
            data["advice"] = advice.group(1).strip()

        return data

    def _extract_measurements(self, text: str) -> Dict[str, Any]:
        """
        Extract numerical measurements with units
        """
        measurements = {}

        # Pattern for measurements
        pattern = r'(\d+\.?\d*)\s*(mm|cm|ml|HU|mg|kg|L|l|cc|mmHg|%|mL)'
        matches = re.finditer(pattern, text, re.IGNORECASE)
        
        for i, m in enumerate(matches):
            measurements[f"measurement_{i+1}"] = {
                "value": float(m.group(1)),
                "unit": m.group(2).lower()
            }

        return measurements

    # ================= UTILS =================

    def _get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get basic file information
        """
        try:
            return {
                "filename": os.path.basename(file_path),
                "file_size_mb": round(os.path.getsize(file_path) / (1024 * 1024), 2),
                "format": os.path.splitext(file_path)[1]
            }
        except:
            return {
                "filename": os.path.basename(file_path),
                "error": "Could not read file info"
            }