# database.py - COMPLETE VERSION (all bugs fixed)

import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
import uuid
import re

class MedicalImageDatabase:
    """
    Proper structured database for medical imaging data
    """
    
    def __init__(self, db_config):
        self.config = db_config
        self.init_database()
    
    def get_connection(self):
        return psycopg2.connect(**self.config)
    
    def init_database(self):
        """
        Create properly structured tables
        """
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Main medical images table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS medical_images (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                patient_id VARCHAR(100) NOT NULL,
                patient_name VARCHAR(200),
                patient_dob DATE,
                patient_age VARCHAR(20),
                patient_sex VARCHAR(10),
                patient_weight NUMERIC(6, 2),

                filename VARCHAR(255),
                file_format VARCHAR(20),
                file_size_mb NUMERIC(10, 2),
                file_path TEXT,
                upload_source VARCHAR(50),

                study_instance_uid VARCHAR(200),
                study_id VARCHAR(100),
                study_date DATE,
                study_time TIME,
                study_description TEXT,
                accession_number VARCHAR(100),

                series_instance_uid VARCHAR(200),
                series_number INTEGER,
                modality VARCHAR(20),
                series_description TEXT,
                body_part_examined VARCHAR(100),
                patient_position VARCHAR(50),

                image_rows INTEGER,
                image_columns INTEGER,
                pixel_spacing VARCHAR(50),
                slice_thickness NUMERIC(10, 4),
                kvp NUMERIC(10, 2),
                exposure_time NUMERIC(10, 2),

                manufacturer VARCHAR(200),
                manufacturer_model VARCHAR(200),
                station_name VARCHAR(200),
                institution_name VARCHAR(200),

                referring_physician VARCHAR(200),
                performing_physician VARCHAR(200),

                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                uploaded_by VARCHAR(100) DEFAULT 'system',

                processing_status VARCHAR(50) DEFAULT 'completed'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clinical_findings (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                image_id UUID REFERENCES medical_images(id) ON DELETE CASCADE,

                report_date DATE,
                report_type VARCHAR(100),

                chief_complaint TEXT,
                clinical_history TEXT,
                findings TEXT,
                impression TEXT,
                recommendations TEXT,

                findings_json JSONB,

                reported_by VARCHAR(200),
                verified_by VARCHAR(200),

                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                image_id UUID REFERENCES medical_images(id) ON DELETE CASCADE,

                measurement_type VARCHAR(100),
                measurement_name VARCHAR(200),
                measurement_value NUMERIC(10, 4),
                measurement_unit VARCHAR(20),

                anatomical_location VARCHAR(200),
                measurement_method VARCHAR(100),

                measured_by VARCHAR(100),
                measured_at TIMESTAMP DEFAULT NOW(),

                measurement_json JSONB
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS image_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                image_id UUID REFERENCES medical_images(id) ON DELETE CASCADE,

                dicom_tags JSONB,
                extracted_text JSONB,
                ai_analysis JSONB,

                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_patient_id ON medical_images(patient_id)",
            "CREATE INDEX IF NOT EXISTS idx_patient_name ON medical_images(patient_name)",
            "CREATE INDEX IF NOT EXISTS idx_study_date ON medical_images(study_date)",
            "CREATE INDEX IF NOT EXISTS idx_modality ON medical_images(modality)",
            "CREATE INDEX IF NOT EXISTS idx_body_part ON medical_images(body_part_examined)",
            "CREATE INDEX IF NOT EXISTS idx_study_uid ON medical_images(study_instance_uid)",
            "CREATE INDEX IF NOT EXISTS idx_upload_source ON medical_images(upload_source)",
            "CREATE INDEX IF NOT EXISTS idx_image_metadata_jsonb ON image_metadata USING GIN(dicom_tags)",
            "CREATE INDEX IF NOT EXISTS idx_findings_jsonb ON clinical_findings USING GIN(findings_json)",
        ]
        for idx in indexes:
            cur.execute(idx)

        conn.commit()
        cur.close()
        conn.close()

        print("✓ Database structure created successfully")

    # ==================================================================
    # STORE — DICOM
    # ==================================================================

    def store_dicom_data(self, metadata, file_path, uploaded_by='system'):
        """
        Store DICOM data in properly structured format.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        try:
            patient_info     = metadata.get('patient_information', {})
            study_info       = metadata.get('study_information', {})
            series_info      = metadata.get('series_information', {})
            image_info       = metadata.get('image_information', {})
            equipment_info   = metadata.get('equipment_information', {})
            acquisition_info = metadata.get('acquisition_parameters', {})
            clinical_info    = metadata.get('clinical_information', {})
            file_info        = metadata.get('file_info', {})

            study_date  = self._parse_date(study_info.get('study_date'))
            study_time  = self._parse_time(study_info.get('study_time'))
            patient_dob = self._parse_date(patient_info.get('patient_birth_date'))

            cur.execute("""
                INSERT INTO medical_images (
                    patient_id, patient_name, patient_dob, patient_age, patient_sex,
                    patient_weight, filename, file_format, file_size_mb, file_path,
                    upload_source, study_instance_uid, study_id, study_date, study_time,
                    study_description, accession_number, series_instance_uid, series_number,
                    modality, series_description, body_part_examined, patient_position,
                    image_rows, image_columns, pixel_spacing, slice_thickness,
                    kvp, exposure_time, manufacturer, manufacturer_model,
                    station_name, institution_name, referring_physician,
                    performing_physician, uploaded_by
                ) VALUES (
                    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s, %s
                )
                RETURNING id
            """, (
                patient_info.get('patient_id', 'UNKNOWN'),
                patient_info.get('patient_name', 'Unknown'),
                patient_dob,
                patient_info.get('patient_age'),
                patient_info.get('patient_sex'),
                self._parse_numeric(patient_info.get('patient_weight')),
                file_info.get('filename'),
                file_info.get('format'),
                file_info.get('file_size_mb'),
                file_path,
                'dicom',
                study_info.get('study_instance_uid'),
                study_info.get('study_id'),
                study_date,
                study_time,
                study_info.get('study_description'),
                study_info.get('accession_number'),
                series_info.get('series_instance_uid'),
                self._parse_numeric(series_info.get('series_number'), is_int=True),
                series_info.get('modality'),
                series_info.get('series_description'),
                series_info.get('body_part_examined'),
                series_info.get('patient_position'),
                image_info.get('rows'),
                image_info.get('columns'),
                image_info.get('pixel_spacing'),
                self._parse_numeric(image_info.get('slice_thickness')),
                self._parse_numeric(acquisition_info.get('kvp')),
                self._parse_numeric(acquisition_info.get('exposure_time')),
                equipment_info.get('manufacturer'),
                equipment_info.get('manufacturer_model_name'),
                equipment_info.get('station_name'),
                equipment_info.get('institution_name'),
                clinical_info.get('referring_physician_name'),
                clinical_info.get('performing_physician_name'),
                uploaded_by
            ))

            image_id = cur.fetchone()[0]

            # full metadata blob
            cur.execute("""
                INSERT INTO image_metadata (image_id, dicom_tags)
                VALUES (%s, %s)
            """, (image_id, json.dumps(metadata.get('all_dicom_tags', {}))))

            measurements = metadata.get('measurements_and_annotations', {})
            if measurements:
                self._store_measurements(cur, image_id, measurements)

            conn.commit()
            return str(image_id)

        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to store DICOM data: {str(e)}")
        finally:
            cur.close()
            conn.close()

    # ==================================================================
    # STORE — PDF REPORT
    # ==================================================================

    def store_pdf_report(self, extracted_data, file_path, uploaded_by='system'):
        """
        Store data extracted from PDF medical reports.

        FIX D: unified key access — tries 'patient_information' first
               (normalised output), then falls back to 'patient_info'
               (raw OCR output) so this works regardless of which path
               produced the metadata.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        try:
            file_info    = extracted_data.get('file_info', {})

            # FIX D — accept both key names
            patient_info = (extracted_data.get('patient_information')
                           or extracted_data.get('patient_info', {}))
            report_info  = (extracted_data.get('study_information')
                           or extracted_data.get('report_info', {}))
            clinical_data = (extracted_data.get('clinical_information')
                            or extracted_data.get('clinical_data', {}))

            cur.execute("""
                INSERT INTO medical_images (
                    patient_id, patient_name, filename, file_format,
                    file_size_mb, file_path, upload_source, study_date,
                    study_description, uploaded_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                patient_info.get('patient_id', 'UNKNOWN'),
                patient_info.get('patient_name', 'Unknown'),
                file_info.get('filename'),
                'PDF',
                file_info.get('file_size_mb'),
                file_path,
                'pdf',
                self._parse_date(report_info.get('report_date')
                                 or report_info.get('study_date')),
                report_info.get('report_type') or report_info.get('study_description'),
                uploaded_by
            ))

            image_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO clinical_findings (
                    image_id, report_date, report_type, chief_complaint,
                    clinical_history, findings, impression, recommendations,
                    findings_json, reported_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                image_id,
                self._parse_date(report_info.get('report_date')
                                 or report_info.get('study_date')),
                report_info.get('report_type'),
                clinical_data.get('chief_complaint'),
                clinical_data.get('clinical_history'),
                clinical_data.get('findings') or clinical_data.get('observations'),
                clinical_data.get('impression'),
                clinical_data.get('recommendations') or clinical_data.get('advice'),
                json.dumps(clinical_data),
                report_info.get('reported_by')
            ))

            cur.execute("""
                INSERT INTO image_metadata (image_id, extracted_text)
                VALUES (%s, %s)
            """, (image_id, json.dumps(extracted_data)))

            measurements = self._extract_measurements_from_text(
                clinical_data.get('findings') or clinical_data.get('observations') or ''
            )
            if measurements:
                self._store_measurements(cur, image_id, measurements)

            conn.commit()
            return str(image_id)

        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to store PDF report: {str(e)}")
        finally:
            cur.close()
            conn.close()

    # ==================================================================
    # STORE — OCR IMAGE REPORT   (Bug A — was completely missing)
    # ==================================================================

    def store_image_report(self, metadata, file_path, uploaded_by='system'):
        """
        Store OCR-extracted image report.
        Reads the normalised keys that
        MetadataExtractor._normalize_ocr_result() produces.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        try:
            patient_info  = metadata.get('patient_information', {})
            study_info    = metadata.get('study_information', {})
            clinical_info = metadata.get('clinical_information', {})
            file_info     = metadata.get('file_info', {})

            cur.execute("""
                INSERT INTO medical_images (
                    patient_id, patient_name, patient_age, patient_sex,
                    filename, file_format, file_size_mb, file_path,
                    upload_source, study_date, study_description,
                    modality, body_part_examined,
                    uploaded_by, processing_status
                ) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s, %s,%s)
                RETURNING id
            """, (
                patient_info.get('patient_id',   'UNKNOWN'),
                patient_info.get('patient_name', 'Unknown'),
                patient_info.get('patient_age'),
                patient_info.get('patient_sex'),
                file_info.get('filename'),
                file_info.get('format'),
                file_info.get('file_size_mb'),
                file_path,
                'ocr_image',
                self._parse_date(study_info.get('study_date')),
                study_info.get('study_description'),
                study_info.get('modality'),
                study_info.get('study_description'),   # body_part = study_description for OCR (e.g. "CHEST")
                uploaded_by,
                'completed'
            ))

            image_id = cur.fetchone()[0]

            # clinical findings row — only if we actually have content
            if clinical_info and any(v and v != 'N/A' for v in clinical_info.values()):
                cur.execute("""
                    INSERT INTO clinical_findings (
                        image_id, report_date, report_type,
                        clinical_history, findings, impression, recommendations,
                        findings_json
                    ) VALUES (%s,%s,%s, %s,%s,%s,%s, %s)
                """, (
                    image_id,
                    self._parse_date(study_info.get('study_date')),
                    study_info.get('report_type'),
                    clinical_info.get('clinical_brief'),
                    clinical_info.get('observations'),
                    clinical_info.get('impression'),
                    clinical_info.get('advice'),
                    json.dumps(clinical_info)
                ))

            # full raw text blob
            cur.execute("""
                INSERT INTO image_metadata (image_id, extracted_text)
                VALUES (%s, %s)
            """, (image_id, json.dumps(metadata)))

            conn.commit()
            return str(image_id)

        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to store image report: {str(e)}")
        finally:
            cur.close()
            conn.close()

    # ==================================================================
    # READ — query methods   (Bug C — all four were missing)
    # ==================================================================

    def get_all_images(self, limit=100):
        """Return the most recent rows from medical_images."""
        conn = self.get_connection()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute("""
                SELECT * FROM medical_images
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            cur.close()
            conn.close()

    def get_image_by_id(self, image_id):
        """Return one image row plus its related findings/measurements."""
        conn = self.get_connection()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute("SELECT * FROM medical_images WHERE id = %s", (image_id,))
            image = cur.fetchone()
            if not image:
                return None

            cur.execute("SELECT * FROM clinical_findings WHERE image_id = %s", (image_id,))
            findings = cur.fetchall()

            cur.execute("SELECT * FROM measurements WHERE image_id = %s", (image_id,))
            measurements = cur.fetchall()

            cur.execute("SELECT * FROM image_metadata WHERE image_id = %s", (image_id,))
            meta = cur.fetchone()

            return {
                'image':           dict(image),
                'clinical_findings': [dict(f) for f in findings],
                'measurements':    [dict(m) for m in measurements],
                'metadata':        dict(meta) if meta else None,
            }
        finally:
            cur.close()
            conn.close()

    def search_images(self, modality=None, body_part=None, patient_id=None):
        """Filter medical_images by any combination of the three fields."""
        conn = self.get_connection()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        try:
            clauses = ["1=1"]
            params  = []

            if modality:
                clauses.append("LOWER(modality) = LOWER(%s)")
                params.append(modality)
            if body_part:
                clauses.append("LOWER(body_part_examined) LIKE LOWER(%s)")
                params.append(f"%{body_part}%")
            if patient_id:
                clauses.append("patient_id = %s")
                params.append(patient_id)

            query = "SELECT * FROM medical_images WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC"
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def get_statistics(self):
        """Aggregate stats used by /api/stats."""
        conn = self.get_connection()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS total FROM medical_images")
            total = cur.fetchone()['total']

            cur.execute("""
                SELECT upload_source, COUNT(*) AS cnt
                FROM medical_images
                GROUP BY upload_source
            """)
            by_source = {r['upload_source']: r['cnt'] for r in cur.fetchall()}

            cur.execute("""
                SELECT modality, COUNT(*) AS cnt
                FROM medical_images
                WHERE modality IS NOT NULL
                GROUP BY modality
            """)
            by_modality = {r['modality']: r['cnt'] for r in cur.fetchall()}

            cur.execute("""
                SELECT body_part_examined, COUNT(*) AS cnt
                FROM medical_images
                WHERE body_part_examined IS NOT NULL
                GROUP BY body_part_examined
            """)
            by_body_part = {r['body_part_examined']: r['cnt'] for r in cur.fetchall()}

            cur.execute("SELECT COUNT(*) AS cnt FROM clinical_findings")
            total_findings = cur.fetchone()['cnt']

            return {
                'total_images':      total,
                'by_upload_source':  by_source,
                'by_modality':       by_modality,
                'by_body_part':      by_body_part,
                'total_findings':    total_findings,
            }
        finally:
            cur.close()
            conn.close()

    def get_structured_data(self, image_id):
        """
        Retrieve all structured data for an image (used in the upload
        response to show what was actually written).
        """
        conn = self.get_connection()
        cur  = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM medical_images WHERE id = %s", (image_id,))
        image_data = cur.fetchone()

        cur.execute("SELECT * FROM clinical_findings WHERE image_id = %s", (image_id,))
        findings = cur.fetchall()

        cur.execute("SELECT * FROM measurements WHERE image_id = %s", (image_id,))
        measurements = cur.fetchall()

        cur.execute("SELECT * FROM image_metadata WHERE image_id = %s", (image_id,))
        metadata = cur.fetchone()

        cur.close()
        conn.close()

        return {
            'image_data':        dict(image_data) if image_data else None,
            'clinical_findings': [dict(f) for f in findings],
            'measurements':      [dict(m) for m in measurements],
            'metadata':          dict(metadata) if metadata else None,
        }

    # ==================================================================
    # HELPERS
    # ==================================================================

    def _store_measurements(self, cur, image_id, measurements):
        for measurement_name, measurement_data in measurements.items():
            if isinstance(measurement_data, dict):
                cur.execute("""
                    INSERT INTO measurements (
                        image_id, measurement_name, measurement_value,
                        measurement_unit, measurement_json
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    image_id,
                    measurement_name,
                    self._parse_numeric(measurement_data.get('value')),
                    measurement_data.get('unit'),
                    json.dumps(measurement_data)
                ))
            elif isinstance(measurement_data, str):
                match = re.search(r'([\d.]+)\s*([a-zA-Z]+)', measurement_data)
                if match:
                    value, unit = match.groups()
                    cur.execute("""
                        INSERT INTO measurements (
                            image_id, measurement_name, measurement_value,
                            measurement_unit, measurement_json
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        image_id,
                        measurement_name,
                        float(value),
                        unit,
                        json.dumps({'raw_value': measurement_data})
                    ))

    def _extract_measurements_from_text(self, findings_text):
        measurements = {}
        patterns = [
            (r'(?:size|diameter|length|width|height):\s*([\d.]+)\s*(mm|cm)', 'size'),
            (r'([\d.]+)\s*x\s*([\d.]+)\s*(mm|cm)', 'dimensions'),
            (r'volume:\s*([\d.]+)\s*(ml|cc|L)', 'volume'),
            (r'density:\s*([\d.]+)\s*HU', 'density'),
        ]
        for pattern, measurement_type in patterns:
            for i, match in enumerate(re.finditer(pattern, findings_text, re.IGNORECASE), 1):
                measurements[f'{measurement_type}_{i}'] = {
                    'value': match.group(1),
                    'unit':  match.group(2) if len(match.groups()) > 1 else None,
                    'type':  measurement_type,
                }
        return measurements

    # ------------------------------------------------------------------
    # _parse_date   —  Bug B fix: handles "22-Nov-2025" and other formats
    # ------------------------------------------------------------------
    def _parse_date(self, date_str):
        """
        Parse every date format the system can produce and return a
        string in YYYY-MM-DD (what psycopg2 needs for a DATE column),
        or None if nothing matched.
        """
        if not date_str or date_str in ('N/A', 'Not Available', ''):
            return None

        # Already in the right shape
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        # YYYYMMDD  (DICOM native)
        if re.match(r'^\d{8}$', date_str):
            return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # Try every other format we might encounter
        formats = [
            "%d-%b-%Y",   # 22-Nov-2025   ← OCR output
            "%d/%b/%Y",   # 22/Nov/2025
            "%d-%m-%Y",   # 22-11-2025
            "%d/%m/%Y",   # 22/11/2025
            "%m-%d-%Y",   # 11-22-2025
            "%m/%d/%Y",   # 11/22/2025
            "%b %d, %Y",  # Nov 22, 2025
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None   # nothing matched — column stays NULL

    def _parse_time(self, time_str):
        if not time_str or time_str in ('N/A', 'Not Available', ''):
            return None
        if len(time_str) >= 6 and time_str[:6].isdigit():
            return f"{time_str[0:2]}:{time_str[2:4]}:{time_str[4:6]}"
        return None

    def _parse_numeric(self, value, is_int=False):
        if value is None or str(value).strip() in ('', 'N/A', 'Not Available'):
            return None
        try:
            return int(float(str(value))) if is_int else float(str(value))
        except (ValueError, TypeError):
            return None