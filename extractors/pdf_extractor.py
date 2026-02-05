# pdf_extractor.py

import PyPDF2
import re
from datetime import datetime
from typing import Dict, Any

class MedicalPDFExtractor:
    """
    Extract structured information from medical PDF reports
    """
    
    def extract(self, file_path: str) -> Dict[str, Any]:
        """
        Extract all information from PDF report
        """
        extracted_data = {
            'file_info': self._get_file_info(file_path),
            'patient_info': {},
            'report_info': {},
            'clinical_data': {},
            'measurements': {},
            'raw_text': ''
        }
        
        # Extract all text from PDF
        with open(file_path, 'rb') as f:
            pdf = PyPDF2.PdfReader(f)
            
            full_text = ''
            for page in pdf.pages:
                full_text += page.extract_text() + '\n'
            
            extracted_data['raw_text'] = full_text
        
        # Parse patient information
        extracted_data['patient_info'] = self._extract_patient_info(full_text)
        
        # Parse report information
        extracted_data['report_info'] = self._extract_report_info(full_text)
        
        # Parse clinical data
        extracted_data['clinical_data'] = self._extract_clinical_data(full_text)
        
        # Extract measurements
        extracted_data['measurements'] = self._extract_measurements(full_text)
        
        return extracted_data
    
    def _get_file_info(self, file_path):
        """Get basic file information"""
        import os
        return {
            'filename': os.path.basename(file_path),
            'file_size_mb': round(os.path.getsize(file_path) / (1024 * 1024), 2)
        }
    
    def _extract_patient_info(self, text: str) -> Dict[str, str]:
        """
        Extract patient information from text
        """
        patient_info = {}
        
        # Patient ID
        match = re.search(r'Patient\s*ID[:\s]+([\w-]+)', text, re.IGNORECASE)
        if match:
            patient_info['patient_id'] = match.group(1).strip()
        
        # Patient Name
        match = re.search(r'Patient\s*Name[:\s]+([A-Za-z\s]+?)(?:\n|Patient)', text, re.IGNORECASE)
        if match:
            patient_info['patient_name'] = match.group(1).strip()
        
        # Age
        match = re.search(r'Age[:\s]+(\d+)', text, re.IGNORECASE)
        if match:
            patient_info['age'] = match.group(1)
        
        # Sex/Gender
        match = re.search(r'(?:Sex|Gender)[:\s]+(Male|Female|M|F)', text, re.IGNORECASE)
        if match:
            patient_info['sex'] = match.group(1).upper()[0]
        
        return patient_info
    
    def _extract_report_info(self, text: str) -> Dict[str, str]:
        """
        Extract report metadata
        """
        report_info = {}
        
        # Report Date
        date_patterns = [
            r'(?:Report\s*Date|Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})'
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                report_info['report_date'] = match.group(1)
                break
        
        # Report Type
        type_patterns = [
            r'(CT\s*Scan|MRI|X-Ray|Ultrasound|Radiology)\s*Report',
            r'Report\s*Type[:\s]+([A-Za-z\s]+)'
        ]
        for pattern in type_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                report_info['report_type'] = match.group(1).strip()
                break
        
        # Reported By (Radiologist)
        match = re.search(r'(?:Reported\s*By|Radiologist)[:\s]+([A-Za-z.\s]+?)(?:\n|,)', text, re.IGNORECASE)
        if match:
            report_info['reported_by'] = match.group(1).strip()
        
        return report_info
    
    def _extract_clinical_data(self, text: str) -> Dict[str, str]:
        """
        Extract clinical findings and impressions
        """
        clinical_data = {}
        
        # Chief Complaint
        match = re.search(r'(?:Chief\s*Complaint|Complaint)[:\s]+(.+?)(?:\n\n|\n[A-Z])', text, re.IGNORECASE | re.DOTALL)
        if match:
            clinical_data['chief_complaint'] = match.group(1).strip()
        
        # Clinical History
        match = re.search(r'(?:Clinical\s*History|History)[:\s]+(.+?)(?:\n\n|\n[A-Z])', text, re.IGNORECASE | re.DOTALL)
        if match:
            clinical_data['clinical_history'] = match.group(1).strip()
        
        # Findings
        match = re.search(r'(?:Findings|Observation)[:\s]+(.+?)(?:\n\n|Impression)', text, re.IGNORECASE | re.DOTALL)
        if match:
            clinical_data['findings'] = match.group(1).strip()
        
        # Impression/Diagnosis
        match = re.search(r'(?:Impression|Diagnosis)[:\s]+(.+?)(?:\n\n|Recommendation)', text, re.IGNORECASE | re.DOTALL)
        if match:
            clinical_data['impression'] = match.group(1).strip()
        
        # Recommendations
        match = re.search(r'(?:Recommendation|Advice)[s]?[:\s]+(.+?)(?:\n\n|\Z)', text, re.IGNORECASE | re.DOTALL)
        if match:
            clinical_data['recommendations'] = match.group(1).strip()
        
        return clinical_data
    
    def _extract_measurements(self, text: str) -> Dict[str, Any]:
        """
        Extract all measurements from text
        """
        measurements = {}
        
        # Size measurements
        size_matches = re.finditer(
            r'(?:size|diameter|length|width|height|dimension)[:\s]*([\d.]+)\s*([xX×]\s*[\d.]+)?[\s]*(mm|cm|m)',
            text,
            re.IGNORECASE
        )
        for i, match in enumerate(size_matches):
            measurements[f'size_{i+1}'] = {
                'value': match.group(1),
                'unit': match.group(3),
                'full_text': match.group(0)
            }
        
        # Volume measurements
        volume_matches = re.finditer(
            r'volume[:\s]*([\d.]+)\s*(ml|cc|L)',
            text,
            re.IGNORECASE
        )
        for i, match in enumerate(volume_matches):
            measurements[f'volume_{i+1}'] = {
                'value': match.group(1),
                'unit': match.group(2),
                'full_text': match.group(0)
            }
        
        # Density (HU - Hounsfield Units for CT)
        density_matches = re.finditer(
            r'([\d.]+)\s*HU',
            text
        )
        for i, match in enumerate(density_matches):
            measurements[f'density_{i+1}'] = {
                'value': match.group(1),
                'unit': 'HU',
                'full_text': match.group(0)
            }
        
        # Heart-specific measurements
        heart_matches = re.finditer(
            r'(?:cardiothoracic\s*ratio|CTR)[:\s]*([\d.]+)',
            text,
            re.IGNORECASE
        )
        for i, match in enumerate(heart_matches):
            measurements['cardiothoracic_ratio'] = {
                'value': match.group(1),
                'type': 'ratio',
                'full_text': match.group(0)
            }
        
        return measurements