# Medical Extraction Service (Python)

This service handles:
- DICOM metadata extraction
- OCR-based medical report parsing
- PDF medical summary extraction
- Structured JSON generation
- PostgreSQL storage

## Supported Inputs
- DICOM (.dcm)
- X-ray / CT / MRI images (.jpg, .png)
- Medical PDFs (.pdf) for text based and scanned based as well

## Key Features
- Robust OCR with preprocessing
- Handles OCR errors (1D → ID, 0PD → OPD, etc.)
- Stores structured + raw JSON metadata

## Run
```bash
pip install -r requirements.txt
python app.py
