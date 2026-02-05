# app.py

from flask import Flask, request, jsonify, render_template
from werkzeug.utils import secure_filename
import os
from datetime import datetime
import traceback
from extractors.ocr_report_extractor import OCRReportExtractor
from extractors.metadata_extractor import MetadataExtractor
from database import MedicalImageDatabase

# =========================================================
# App configuration
# =========================================================

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500 MB

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# =========================================================
# Initialize services
# =========================================================

extractor = MetadataExtractor()
ocr_extractor = OCRReportExtractor()

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "medical_imaging",
    "user": "postgres",
    "password": "2072911"   # consider env var later
}

db = MedicalImageDatabase(DB_CONFIG)

# =========================================================
# UI ROUTES
# =========================================================

@app.route("/")
def index():
    return render_template("upload.html")

@app.route("/patient")
def patient_view():
    return render_template("patient_view.html")

@app.route("/doctor")
def doctor_view():
    return render_template("doctor_view.html")

@app.route("/demo")
def demo_view():
    return render_template("demo_view.html")

# =========================================================
# UPLOAD ROUTE
# =========================================================

@app.route("/upload", methods=["POST"])
def upload_medical_file():
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "No file provided"}), 400

        file = request.files["file"]
        uploaded_by = request.form.get("uploaded_by", "system")

        if file.filename == "":
            return jsonify({"success": False, "error": "No file selected"}), 400

        # Save file
        original_name = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{original_name}"
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        print(f"[UPLOAD] Saved file → {filepath}")

        # FIX #1: Moved file_ext here — it was defined AFTER first use before.
        #         The old code referenced file_ext on the OCR fallback check
        #         which ran before this line, crashing with NameError.
        file_ext = os.path.splitext(filename)[1].lower()

        # ===================== EXTRACT METADATA =====================
        print("[UPLOAD] Extracting metadata...")
        metadata = extractor.extract(filepath)   # MetadataExtractor already calls OCR internally for images

        # FIX #2: Removed the broken standalone OCR fallback block.
        #         The old code did:
        #           if (... "error" in metadata) and file_ext in [".jpg", ...]:
        #               metadata = ocr_extractor.extract_from_image(filepath)
        #         This was wrong for two reasons:
        #           (a) file_ext wasn't defined yet at that point → NameError crash
        #           (b) ocr_extractor.extract_from_image() returns raw OCR keys
        #               ("patient_info") but store_dicom_data() expects
        #               normalized keys ("patient_information") — so even if it
        #               didn't crash, the patient data would never reach the DB.
        #         MetadataExtractor.extract_medical_image() already runs OCR
        #         and normalizes the output via _normalize_ocr_result().
        #         No separate fallback is needed here.

        if not metadata or "error" in metadata:
            return jsonify({
                "success": False,
                "error": metadata.get("error", "Metadata extraction failed") if metadata else "Metadata extraction failed"
            }), 500
        # =============================================================

        print(f"[UPLOAD] Extraction method: {metadata.get('processing_info', {}).get('extraction_method', 'unknown')}")
        print(f"[UPLOAD] Patient: {metadata.get('patient_information', {}).get('patient_name', 'N/A')}")

        print("[UPLOAD] Storing in database...")

        # FIX #3: Route to the correct store method based on file type.
        #         The old code sent jpg/png through store_dicom_data() which
        #         expects DICOM-specific keys. OCR-extracted metadata has a
        #         different shape. Route image reports to store_image_report()
        #         instead (defined below as a new method — see database.py note).
        if file_ext in [".dcm", ".dicom", ".dic"]:
            image_id = db.store_dicom_data(metadata, filepath, uploaded_by)

        elif file_ext == ".pdf":
            image_id = db.store_pdf_report(metadata, filepath, uploaded_by)

        elif file_ext in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            image_id = db.store_image_report(metadata, filepath, uploaded_by)

        else:
            return jsonify({"success": False, "error": f"Unsupported format: {file_ext}"}), 400

        structured_data = db.get_structured_data(image_id)

        return jsonify({
            "success": True,
            "image_id": image_id,
            "extracted_metadata": metadata,
            "stored_structure": structured_data,
            "message": "File uploaded and processed successfully"
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# =========================================================
# API ROUTES
# =========================================================

@app.route("/api/images", methods=["GET"])
def get_all_images():
    try:
        images = db.get_all_images(limit=100)
        result = []

        for img in images:
            result.append({
                "id": str(img["id"]),
                "patient_id": img["patient_id"],
                "patient_name": img["patient_name"],
                "filename": img["filename"],
                "file_format": img["file_format"],
                "file_size_mb": float(img["file_size_mb"] or 0),
                "modality": img["modality"],
                "body_part": img["body_part"],
                "study_date": img["study_date"].isoformat() if img["study_date"] else None,
                "uploaded_at": img["uploaded_at"].isoformat() if img["uploaded_at"] else None,
                "uploaded_by": img["uploaded_by"]
            })

        return jsonify({"success": True, "images": result, "count": len(result)}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/images/<image_id>", methods=["GET"])
def get_image_details(image_id):
    try:
        image = db.get_image_by_id(image_id)

        if not image:
            return jsonify({"success": False, "error": "Image not found"}), 404

        return jsonify({
            "success": True,
            "image": image
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/search", methods=["GET"])
def search_images():
    try:
        modality = request.args.get("modality")
        body_part = request.args.get("body_part")
        patient_id = request.args.get("patient_id")

        images = db.search_images(modality, body_part, patient_id)
        return jsonify({"success": True, "images": images}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/stats", methods=["GET"])
def get_statistics():
    try:
        stats = db.get_statistics()
        return jsonify({"success": True, "statistics": stats}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🏥 Medical Imaging System - Complete Prototype")
    print("=" * 70)
    print("📤 Upload:        http://localhost:5000/")
    print("👤 Patient View:  http://localhost:5000/patient")
    print("👨‍⚕️ Doctor View:   http://localhost:5000/doctor")
    print("=" * 70)

    app.run(debug=True, host="0.0.0.0", port=5000)