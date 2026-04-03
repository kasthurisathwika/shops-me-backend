# app.py ✅ RESTAURANT ONLY (swiggy_restaurant schema)
# ------------------------------------------------------------
# ✅ MySQL only
# ✅ GCS Signed URLs for images
# ✅ /media/list endpoint
# ✅ Keeps OLD route names where possible (store + items + orders + rider)
# ✅ Removes categories/subcategories/units (NOT needed for restaurant-only)
# ✅ Uses menu_sections instead of categories
# ✅ Uses menu_item_variants instead of menu_items.price
# ------------------------------------------------------------

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import datetime as dt
from datetime import datetime, time, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import os
import re
import time
import mimetypes
import bcrypt
import secrets
import json
import uuid
from werkzeug.utils import secure_filename
from google.cloud import storage
from urllib.parse import quote_plus

app = Flask(__name__)

CORS(
    app,
    resources={r"/*": {"origins": ["http://localhost:5173", "http://127.0.0.1:5173"]}},
    supports_credentials=False,
)

# ======================
# MYSQL CONFIG
# ======================
DB_USER = "swiggy_user"
DB_PASS = "Swiggy@123"
DB_HOST = "localhost"
DB_NAME = "swiggy_restaurant"   # ✅ IMPORTANT

url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    database=DB_NAME,
    query={"charset": "utf8mb4"},
)

engine = create_engine(url, pool_pre_ping=True)

# ======================
# ✅ GCS CONFIG
# ======================
GCS_BUCKET = "shopsandme_images"
#gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)
cred_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Shops_Me\Backend\shopsandme-5ae10de858cf.json"

gcs_client = None
gcs_bucket = None

try:
    if cred_path:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(GCS_BUCKET)
    else:
        print("⚠️ GOOGLE_APPLICATION_CREDENTIALS not set. GCS features disabled.")
except Exception as e:
    print("⚠️ GCS init failed. GCS features disabled:", e)
    gcs_client = None
    gcs_bucket = None

ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "webp"}


# ======================
# GCS HELPERS
# ======================
def gcs_signed_url(blob_path: str, minutes: int = 120) -> str:
    blob_path = (blob_path or "").strip().lstrip("/")
    if not blob_path or gcs_bucket is None:
        return ""
    blob = gcs_bucket.blob(blob_path)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=minutes),
        method="GET",
    )


def resolve_image_url(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("http://") or v.startswith("https://"):
        return v
    return gcs_signed_url(v, 120)


def allowed_image(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_IMAGE_EXT


def upload_file_to_gcs(file_storage, folder: str) -> str:
    original_name = secure_filename(file_storage.filename or "")
    if not allowed_image(original_name):
        raise ValueError("Only png/jpg/jpeg/webp images allowed")

    ts = int(time.time())
    object_path = f"{folder.rstrip('/')}/{ts}_{original_name}"

    blob = gcs_bucket.blob(object_path)
    content_type = (
        file_storage.mimetype
        or mimetypes.guess_type(original_name)[0]
        or "application/octet-stream"
    )

    blob.upload_from_file(
        file_storage.stream,
        content_type=content_type,
        rewind=True,
    )

    blob.cache_control = "public, max-age=3600"
    blob.patch()

    return object_path


# ======================
# BASIC HELPERS
# ======================
def empty_to_none(v):
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def safe_int(v, default=0):
    v = empty_to_none(v)
    if v is None:
        return default
    try:
        return int(float(v))
    except:
        return default


def safe_int_or_none(v):
    v = empty_to_none(v)
    if v is None:
        return None
    try:
        return int(float(v))
    except:
        return None


def safe_float(v, default=0.0):
    v = empty_to_none(v)
    if v is None:
        return default
    try:
        return float(v)
    except:
        return default


def norm_status(v):
    return str(v or "").strip().lower()


def make_order_number(order_id: int) -> str:
    # example: ORD-20260212-00000123
    return f"ORD-{datetime.now().strftime('%Y%m%d')}-{order_id:08d}"

def parse_delivery_range(v):
    """
    Supports: "30-45 mins", "30 - 45", "45", "45 mins"
    Returns (min, max) or (None, None)
    """
    v = (v or "").strip()
    if not v:
        return (None, None)
    nums = re.findall(r"\d+", v)
    if not nums:
        return (None, None)
    if len(nums) == 1:
        n = int(nums[0])
        return (n, n)
    return (int(nums[0]), int(nums[1]))

def parse_bool_int(v, default=0):
    """
    Accepts: 1/0, true/false, yes/no, veg/nonveg, VEG/NON-VEG
    Returns: 1 or 0
    """
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "veg", "vegetarian"):
        return 1
    if s in ("0", "false", "no", "n", "nonveg", "non-veg", "non_veg", "non vegetarian", "nonvegetarian"):
        return 0
    return default

def bcrypt_hash_password(plain: str) -> str:
    plain = (plain or "").strip()
    hashed = bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt(rounds=12))
    return hashed.decode("utf-8")

def bcrypt_check_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False
        
def safe_iso(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    # if it's already a string
    return str(v)

def _td_to_time(v):
    """Normalize MySQL TIME that may arrive as dt.time, dt.timedelta, or 'HH:MM:SS'."""
    if v is None:
        return None

    if isinstance(v, dt.time):
        return v

    if isinstance(v, dt.timedelta):
        total = int(v.total_seconds()) % (24 * 3600)
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return dt.time(hour=h, minute=m, second=s)

    if isinstance(v, str):
        try:
            parts = v.strip().split(":")
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            s = int(parts[2]) if len(parts) > 2 else 0
            return dt.time(hour=h, minute=m, second=s)
        except Exception:
            return None

    return None

# ======================
# FIXED APP SOUND PATHS
# ======================
VENDOR_SOUND_PATH = "notifications/new_order.mp3"
RIDER_SOUND_PATH = "notifications/rider_new_order.mp3"


def gcs_public_url(blob_path: str) -> str:
    blob_path = (blob_path or "").strip().lstrip("/")
    if not blob_path:
        return ""
    return f"https://storage.googleapis.com/{GCS_BUCKET}/{blob_path}"


def resolve_media_url(value: str, minutes: int = 240, prefer_public: bool = False) -> str:
    """
    For audio/video files.
    Signed URL if private, public URL if public.
    """
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("http://") or v.startswith("https://"):
        return v
    if prefer_public:
        return gcs_public_url(v)
    return gcs_signed_url(v, minutes)


@app.route("/vendor/sound", methods=["GET"])
def vendor_sound():
    return jsonify({
        "ok": True,
        "type": "vendor",
        "soundUrl": resolve_media_url(VENDOR_SOUND_PATH, minutes=240, prefer_public=False)
    }), 200


@app.route("/rider/sound", methods=["GET"])
def rider_sound():
    return jsonify({
        "ok": True,
        "type": "rider",
        "soundUrl": resolve_media_url(RIDER_SOUND_PATH, minutes=240, prefer_public=False)
    }), 200

# ======================
# ✅ MEDIA LIST
# ======================
@app.route("/media/list", methods=["GET"])
def media_list():
    prefix = (request.args.get("prefix") or "").strip()
    if not prefix:
        return jsonify({"error": "prefix is required"}), 400

    if not prefix.endswith("/"):
        prefix += "/"

    try:
        blobs = gcs_client.list_blobs(GCS_BUCKET, prefix=prefix)
        out = []
        for b in blobs:
            if b.name.endswith("/"):
                continue
            out.append({
                "path": b.name,
                "name": b.name.split("/")[-1],
                "url": gcs_signed_url(b.name, 120),
            })
        return jsonify(out), 200
    except Exception as e:
        return jsonify({
            "error": "GCS access failed",
            "bucket": GCS_BUCKET,
            "prefix": prefix,
            "details": str(e)
        }), 500

@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    if origin in ("http://localhost:5173", "http://127.0.0.1:5173"):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return response

# ======================
# STORE ROUTES (Restaurant DB)
# Keep OLD route names
# ======================
@app.route("/admin/add-store", methods=["POST"])
def add_store():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    store_name = str(data.get("store_name", "") or "").strip()
    if not store_name:
        return jsonify({"error": "store_name is required"}), 400

    # -------- owner fields from AddRestaurant page --------
    owner_name = empty_to_none(data.get("owner_name"))
    owner_email = empty_to_none(data.get("owner_email"))
    owner_phone = empty_to_none(data.get("owner_phone"))
    owner_password = empty_to_none(data.get("owner_password"))

    # if frontend sends first/last only, you already combine there, so owner_name comes here fine.

    # if vendor_id explicitly sent, use it, else create vendor if owner fields exist
    vendor_id = safe_int_or_none(data.get("vendor_id"))

    logo_url_value = empty_to_none(data.get("logo_url"))
    cover_url_value = empty_to_none(data.get("cover_url"))

    try:
        if is_multipart and "logo" in request.files:
            f = request.files["logo"]
            if f and f.filename:
                logo_url_value = upload_file_to_gcs(f, folder=f"stores/tmp/logo")

        if is_multipart and "cover" in request.files:
            f = request.files["cover"]
            if f and f.filename:
                cover_url_value = upload_file_to_gcs(f, folder=f"stores/tmp/cover")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    # featured/status (your UI sends booleans sometimes)
    featured_raw = data.get("featured", 0)
    is_featured = 1 if str(featured_raw).strip().lower() in ("1", "true", "yes") else 0

    # delivery_time from UI like "30-45 mins"
    dt_min = safe_int_or_none(data.get("delivery_time_min"))
    dt_max = safe_int_or_none(data.get("delivery_time_max"))
    if dt_min is None and dt_max is None and empty_to_none(data.get("delivery_time")) is not None:
        import re
        nums = re.findall(r"\d+", str(data.get("delivery_time") or ""))
        if len(nums) == 1:
            dt_min = dt_max = int(nums[0])
        elif len(nums) >= 2:
            dt_min, dt_max = int(nums[0]), int(nums[1])

    payload = {
        "vendor_id": None,  # set later
        "store_name": store_name,

        # keep store phone separate if you want; UI uses vendor phone
        "phone": empty_to_none(data.get("phone")),

        "address_line1": empty_to_none(data.get("address")) or empty_to_none(data.get("address_line1")),
        "address_line2": empty_to_none(data.get("address_line2")),
        "city": empty_to_none(data.get("city")) or empty_to_none(data.get("zone")),
        "zone": empty_to_none(data.get("zone")),
        "state": empty_to_none(data.get("state")),
        "pincode": empty_to_none(data.get("pincode")),

        "latitude": safe_float(data.get("latitude"), None) if empty_to_none(data.get("latitude")) is not None else None,
        "longitude": safe_float(data.get("longitude"), None) if empty_to_none(data.get("longitude")) is not None else None,

        "gst_number": empty_to_none(data.get("gst")) or empty_to_none(data.get("gst_number")),

        "status": "ACTIVE",
        "is_featured": is_featured,

        "sort_order": safe_int_or_none(data.get("sort_order")),
        "location": empty_to_none(data.get("location")),

        "delivery_time_min": dt_min,
        "delivery_time_max": dt_max,

        "min_order_value": safe_float(data.get("min_order_value"), 0.0) if empty_to_none(data.get("min_order_value")) is not None else None,
        "packing_charge": safe_float(data.get("packing_charge"), 0.0),

        "logo_url": empty_to_none(logo_url_value),
        "cover_url": empty_to_none(cover_url_value),
    }

    with engine.begin() as conn:
        # 1) Decide vendor_id
        if vendor_id:
            payload["vendor_id"] = vendor_id
        else:
            # Create vendor ONLY if owner fields are provided
            if owner_name or owner_email or owner_phone or owner_password:
                conn.execute(text("""
                    INSERT INTO vendors (name, email, phone, password_hash, status)
                    VALUES (:name, :email, :phone, :pass, 'ACTIVE')
                """), {
                    "name": owner_name,
                    "email": owner_email,
                    "phone": owner_phone,
                    "pass": owner_password,
                })
                payload["vendor_id"] = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        # 2) Insert store
        conn.execute(text("""
            INSERT INTO stores
            (vendor_id, store_name, phone,
             address_line1, address_line2, city, zone, state, pincode,
             latitude, longitude, gst_number,
             status, is_featured, sort_order, location,
             delivery_time_min, delivery_time_max,
             min_order_value, packing_charge, logo_url, cover_url)
            VALUES
            (:vendor_id, :store_name, :phone,
             :address_line1, :address_line2, :city, :zone, :state, :pincode,
             :latitude, :longitude, :gst_number,
             :status, :is_featured, :sort_order, :location,
             :delivery_time_min, :delivery_time_max,
             :min_order_value, :packing_charge, :logo_url, :cover_url)
        """), payload)

        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

    return jsonify({
        "message": "Store Added Successfully",
        "store_id": new_id
    }), 201

@app.route("/admin/get-stores", methods=["GET"])
def get_stores():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.*,
              v.name  AS owner_name,
              v.phone AS owner_phone,
              v.email AS owner_email,
              v.password_hash AS owner_password
            FROM stores s
            LEFT JOIN vendors v ON v.vendor_id = s.vendor_id
            ORDER BY s.store_id DESC
        """)).mappings().all()

    out = []
    for r in rows:
        d = dict(r)

        # ✅ IMPORTANT: your UI has ONLY one address input -> return address_line1
        d["address"] = d.get("address_line1") or ""

        # ✅ UI expects gst + delivery_time + featured(boolean) + status(boolean)
        d["gst"] = d.get("gst_number") or ""

        mn = d.get("delivery_time_min")
        mx = d.get("delivery_time_max")
        if mn is None and mx is None:
            d["delivery_time"] = ""
        elif mx is None or mx == mn:
            d["delivery_time"] = f"{mn} mins"
        else:
            d["delivery_time"] = f"{mn}-{mx} mins"

        d["featured"] = True if int(d.get("is_featured") or 0) == 1 else False
        d["status"] = True if str(d.get("status")) == "ACTIVE" else False

        # signed urls
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["cover_url"] = resolve_image_url(d.get("cover_url"))

        out.append(d)

    return jsonify(out), 200

@app.route("/admin/get-store/<int:store_id>", methods=["GET"])
def get_store_by_id(store_id):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
              s.*,
              v.name  AS owner_name,
              v.phone AS owner_phone,
              v.email AS owner_email,
              v.password_hash AS owner_password
            FROM stores s
            LEFT JOIN vendors v ON v.vendor_id = s.vendor_id
            WHERE s.store_id = :sid
            LIMIT 1
        """), {"sid": store_id}).mappings().first()

    if not row:
        return jsonify({"error": "Store not found"}), 404

    d = dict(row)
    d["address"] = d.get("address_line1") or ""
    d["gst"] = d.get("gst_number") or ""

    mn = d.get("delivery_time_min")
    mx = d.get("delivery_time_max")
    if mn is None and mx is None:
        d["delivery_time"] = ""
    elif mx is None or mx == mn:
        d["delivery_time"] = f"{mn} mins"
    else:
        d["delivery_time"] = f"{mn}-{mx} mins"

    d["featured"] = True if int(d.get("is_featured") or 0) == 1 else False
    d["status"] = True if str(d.get("status")) == "ACTIVE" else False

    d["logo_url"] = resolve_image_url(d.get("logo_url"))
    d["cover_url"] = resolve_image_url(d.get("cover_url"))
    return jsonify(d), 200

@app.route("/admin/update-store/<int:store_id>", methods=["PUT"])
def update_store(store_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    updates = {}

    # ----------------------------
    # STORE FIELDS (stores table)
    # ----------------------------
    if "store_name" in data:
        updates["store_name"] = empty_to_none(str(data.get("store_name") or "").strip())

    # phone can come as phone OR owner_phone
    if "phone" in data:
        updates["phone"] = empty_to_none(str(data.get("phone") or "").strip())
    if "owner_phone" in data:
        updates["phone"] = empty_to_none(str(data.get("owner_phone") or "").strip())

    # zone (your table has zone)
    if "zone" in data:
        updates["zone"] = empty_to_none(str(data.get("zone") or "").strip())
        # keep city sync (optional)
        if "city" not in updates:
            updates["city"] = updates["zone"]

    # address (frontend sends "address")
    if "address" in data or "address_line1" in data:
        updates["address_line1"] = empty_to_none(data.get("address")) or empty_to_none(data.get("address_line1"))

    if "address_line2" in data:
        updates["address_line2"] = empty_to_none(data.get("address_line2"))
    if "city" in data:
        updates["city"] = empty_to_none(data.get("city"))
    if "state" in data:
        updates["state"] = empty_to_none(data.get("state"))
    if "pincode" in data:
        updates["pincode"] = empty_to_none(data.get("pincode"))

    if "latitude" in data:
        updates["latitude"] = safe_float(data.get("latitude"), None) if empty_to_none(data.get("latitude")) is not None else None
    if "longitude" in data:
        updates["longitude"] = safe_float(data.get("longitude"), None) if empty_to_none(data.get("longitude")) is not None else None

    if "gst" in data or "gst_number" in data:
        updates["gst_number"] = empty_to_none(data.get("gst")) or empty_to_none(data.get("gst_number"))

    if "featured" in data:
        v = data.get("featured")
        updates["is_featured"] = 1 if str(v).strip().lower() in ("1", "true", "yes") else 0

    if "status" in data:
        v = data.get("status")
        updates["status"] = "ACTIVE" if str(v).strip().lower() in ("1", "true", "yes", "active") else "INACTIVE"

    # sort_order, location (your table has them)
    if "sort_order" in data:
        updates["sort_order"] = safe_int_or_none(data.get("sort_order"))
    # If frontend sends structured settings
    if "settings" in data:
        updates["settings_json"] = json.dumps(data.get("settings"))

    # If frontend sends simple location label
    if "location_label" in data:
        updates["location"] = empty_to_none(str(data.get("location_label") or "").strip())

    # delivery_time parse
    if "delivery_time_min" in data or "delivery_time" in data or "delivery_time_max" in data:
        dt_min = safe_int_or_none(data.get("delivery_time_min"))
        dt_max = safe_int_or_none(data.get("delivery_time_max"))

        if dt_min is None and dt_max is None and "delivery_time" in data:
            import re
            nums = re.findall(r"\d+", str(data.get("delivery_time") or ""))
            if len(nums) == 1:
                dt_min = dt_max = int(nums[0])
            elif len(nums) >= 2:
                dt_min, dt_max = int(nums[0]), int(nums[1])

        updates["delivery_time_min"] = dt_min
        updates["delivery_time_max"] = dt_max

    if "packing_charge" in data:
        updates["packing_charge"] = safe_float(data.get("packing_charge"), 0.0)

    # file upload
    try:
        if is_multipart and "logo" in request.files:
            f = request.files["logo"]
            if f and f.filename:
                updates["logo_url"] = upload_file_to_gcs(f, folder=f"stores/{store_id}/logo")

        if is_multipart and "cover" in request.files:
            f = request.files["cover"]
            if f and f.filename:
                updates["cover_url"] = upload_file_to_gcs(f, folder=f"stores/{store_id}/cover")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    # ----------------------------
    # OWNER FIELDS (vendors table)
    # ----------------------------
    owner_updates = {}
    if "owner_name" in data:
        owner_updates["name"] = empty_to_none(str(data.get("owner_name") or "").strip())
    if "owner_email" in data:
        owner_updates["email"] = empty_to_none(str(data.get("owner_email") or "").strip())
    if "owner_phone" in data:
        owner_updates["phone"] = empty_to_none(str(data.get("owner_phone") or "").strip())
    if "owner_password" in data:
        owner_updates["password_hash"] = empty_to_none(str(data.get("owner_password") or "").strip())

    # ✅ allow updating ONLY owner fields too
    if not updates and not owner_updates:
        return jsonify({"error": "No valid fields to update"}), 400

    with engine.begin() as conn:
        # ensure store exists + get vendor_id
        base = conn.execute(
            text("SELECT store_id, vendor_id FROM stores WHERE store_id = :sid"),
            {"sid": store_id}
        ).mappings().first()

        if not base:
            return jsonify({"error": "Store not found"}), 404

        current_vid = base.get("vendor_id")

        # 1) vendor create/update
        if owner_updates:
            if not current_vid:
                # create vendor
                conn.execute(text("""
                    INSERT INTO vendors (name, email, phone, password_hash, status)
                    VALUES (:name, :email, :phone, :pass, 'ACTIVE')
                """), {
                    "name": owner_updates.get("name"),
                    "email": owner_updates.get("email"),
                    "phone": owner_updates.get("phone"),
                    "pass": owner_updates.get("password_hash"),
                })
                current_vid = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

                # attach to store
                conn.execute(
                    text("UPDATE stores SET vendor_id = :vid WHERE store_id = :sid"),
                    {"vid": current_vid, "sid": store_id}
                )
            else:
                # update vendor
                owner_updates["vid"] = int(current_vid)
                setv = ", ".join([f"{k} = :{k}" for k in owner_updates.keys() if k != "vid"])
                conn.execute(
                    text(f"UPDATE vendors SET {setv} WHERE vendor_id = :vid"),
                    owner_updates
                )

        # 2) store update
        if updates:
            updates["sid"] = store_id
            set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "sid"])
            conn.execute(
                text(f"UPDATE stores SET {set_clause} WHERE store_id = :sid"),
                updates
            )

        # 3) fetch FINAL row WITH JOIN
        row = conn.execute(text("""
            SELECT
              s.*,
              v.name AS owner_name,
              v.phone AS owner_phone,
              v.email AS owner_email,
              v.password_hash AS owner_password
            FROM stores s
            LEFT JOIN vendors v ON v.vendor_id = s.vendor_id
            WHERE s.store_id = :sid
            LIMIT 1
        """), {"sid": store_id}).mappings().first()

    d = dict(row)
    d["logo_url"] = resolve_image_url(d.get("logo_url"))
    d["cover_url"] = resolve_image_url(d.get("cover_url"))
    d["featured"] = int(d.get("is_featured") or 0)
    d["address"] = " ".join([x for x in [
        d.get("address_line1"), d.get("address_line2"),
        d.get("city"), d.get("state"), d.get("pincode")
    ] if x])

    return jsonify(d), 200

@app.route("/admin/edit-store/<int:store_id>", methods=["PUT"])
def admin_edit_store(store_id):
    return update_store(store_id)


@app.route("/admin/delete-store/<int:store_id>", methods=["DELETE"])
def delete_store(store_id):
    try:
        with engine.begin() as conn:
            # ✅ get vendor_id
            row = conn.execute(
                text("SELECT vendor_id FROM stores WHERE store_id = :sid"),
                {"sid": store_id}
            ).mappings().first()

            if not row:
                return jsonify({"error": "Store Not Found"}), 404

            vendor_id = row.get("vendor_id")

            # ==========================================================
            # ✅ DELETE ORDER GRAPH FIRST (deepest children → parents)
            # orders(store_id) -> order_items -> order_item_addons
            # orders(store_id) -> deliveries
            # orders(store_id) -> payments
            # ==========================================================

            # order_item_addons (depends on order_items)
            conn.execute(text("""
                DELETE oia
                FROM order_item_addons oia
                JOIN order_items oi ON oi.order_item_id = oia.order_item_id
                JOIN orders o ON o.order_id = oi.order_id
                WHERE o.store_id = :sid
            """), {"sid": store_id})

            # order_items (depends on orders)
            conn.execute(text("""
                DELETE oi
                FROM order_items oi
                JOIN orders o ON o.order_id = oi.order_id
                WHERE o.store_id = :sid
            """), {"sid": store_id})

            # deliveries (depends on orders)
            conn.execute(text("""
                DELETE d
                FROM deliveries d
                JOIN orders o ON o.order_id = d.order_id
                WHERE o.store_id = :sid
            """), {"sid": store_id})

            # payments (depends on orders)
            conn.execute(text("""
                DELETE p
                FROM payments p
                JOIN orders o ON o.order_id = p.order_id
                WHERE o.store_id = :sid
            """), {"sid": store_id})

            # orders
            conn.execute(text("DELETE FROM orders WHERE store_id = :sid"), {"sid": store_id})


            # ==========================================================
            # ✅ DELETE MENU GRAPH (deepest children → parents)
            # menu_items(store_id) -> menu_item_variants
            # menu_items -> menu_item_addon_groups
            # menu_item_addon_groups -> addon_group_items
            # addon_group_items -> addons
            # ==========================================================

            # menu_item_variants
            conn.execute(text("""
                DELETE miv
                FROM menu_item_variants miv
                JOIN menu_items mi ON mi.menu_item_id = miv.menu_item_id
                WHERE mi.store_id = :sid
            """), {"sid": store_id})

            # menu_item_addon_groups (depends on menu_items)
            conn.execute(text("""
                DELETE miag
                FROM menu_item_addon_groups miag
                JOIN menu_items mi ON mi.menu_item_id = miag.menu_item_id
                WHERE mi.store_id = :sid
            """), {"sid": store_id})

            # order safety: delete addons linked to store's addon groups
            # addon_group_items depends on addon_groups; may also depend on addons
            # We'll delete addon_group_items for store's addon_groups first:
            conn.execute(text("""
                DELETE agi
                FROM addon_group_items agi
                JOIN addon_groups ag ON ag.addon_group_id = agi.addon_group_id
                WHERE ag.store_id = :sid
            """), {"sid": store_id})

            # delete addons if they belong to store (if addons has store_id)
            # If your addons table does not have store_id, this will fail → ignore safely.
            try:
                conn.execute(text("DELETE FROM addons WHERE store_id = :sid"), {"sid": store_id})
            except Exception:
                pass

            # addon_groups
            try:
                conn.execute(text("DELETE FROM addon_groups WHERE store_id = :sid"), {"sid": store_id})
            except Exception:
                pass

            # menu_items
            conn.execute(text("DELETE FROM menu_items WHERE store_id = :sid"), {"sid": store_id})

            # menu_sections (depends on stores)
            conn.execute(text("DELETE FROM menu_sections WHERE store_id = :sid"), {"sid": store_id})


            # ==========================================================
            # ✅ OTHER STORE-DEPENDENT TABLES
            # ==========================================================

            # store_hours
            try:
                conn.execute(text("DELETE FROM store_hours WHERE store_id = :sid"), {"sid": store_id})
            except Exception:
                pass

            # banners
            try:
                conn.execute(text("DELETE FROM banners WHERE store_id = :sid"), {"sid": store_id})
            except Exception:
                pass

            # curation_stores mapping
            try:
                conn.execute(text("DELETE FROM curation_stores WHERE store_id = :sid"), {"sid": store_id})
            except Exception:
                pass


            # ==========================================================
            # ✅ FINALLY delete store
            # ==========================================================
            conn.execute(text("DELETE FROM stores WHERE store_id = :sid"), {"sid": store_id})


            # ==========================================================
            # ✅ delete vendor (only if no stores left)
            # ==========================================================
            if vendor_id:
                other = conn.execute(text("""
                    SELECT COUNT(*) AS cnt
                    FROM stores
                    WHERE vendor_id = :vid
                """), {"vid": vendor_id}).mappings().first()

                if int(other["cnt"]) == 0:
                    conn.execute(text("DELETE FROM vendors WHERE vendor_id = :vid"), {"vid": vendor_id})

        return jsonify({"message": "Deleted Successfully"}), 200

    except Exception as e:
        return jsonify({"error": "Delete failed", "details": str(e)}), 500

# ======================
# ✅ ZONES (Admin Master)
# ======================

def _zone_row_to_api(r):
    d = dict(r)
    # normalize keys for frontend
    d["zoneId"] = int(d.pop("zone_id"))
    d["zoneCode"] = d.pop("zone_code") or ""
    d["name"] = d.get("name") or ""
    d["email"] = d.get("email") or ""
    d["phone"] = d.get("phone") or ""
    d["status"] = "Active" if (d.get("status") or "ACTIVE") == "ACTIVE" else "Inactive"

    d["digitalPayment"] = True if int(d.get("digital_payment") or 0) == 1 else False
    d["cod"] = True if int(d.get("cod") or 0) == 1 else False
    d["offlinePayment"] = True if int(d.get("offline_payment") or 0) == 1 else False

    d["deliveryChargeActive"] = True if int(d.get("delivery_charge_active") or 0) == 1 else False
    d["deliveryChargeAmount"] = float(d.get("delivery_charge_amount") or 0)
    d["deliveryChargeMessage"] = d.get("delivery_charge_message") or ""

    # JSON columns may come as str depending on driver; make safe
    sm = d.get("selected_modules_json")
    ms = d.get("module_settings_json")
    try:
        d["selectedModules"] = json.loads(sm) if isinstance(sm, str) else (sm or [])
    except Exception:
        d["selectedModules"] = []
    try:
        d["moduleSettings"] = json.loads(ms) if isinstance(ms, str) else (ms or [])
    except Exception:
        d["moduleSettings"] = []

    # counts from your existing tables (stores/riders)
    d["stores"] = int(d.get("stores") or 0)
    d["deliverymen"] = int(d.get("deliverymen") or 0)

    # cleanup db-only
    for k in [
        "digital_payment","offline_payment","delivery_charge_active",
        "delivery_charge_amount","delivery_charge_message",
        "selected_modules_json","module_settings_json"
    ]:
        d.pop(k, None)

    return d


@app.route("/admin/zones", methods=["GET"])
def admin_list_zones():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              z.*,
              (SELECT COUNT(*)
                 FROM stores s
                WHERE s.zone COLLATE utf8mb4_unicode_ci = z.name
              ) AS stores,
              (SELECT COUNT(*)
                 FROM riders r
                WHERE r.zone = z.name
              ) AS deliverymen
            FROM zones z
            ORDER BY z.zone_id DESC
        """)).mappings().all()

    return jsonify([_zone_row_to_api(r) for r in rows]), 200

@app.route("/admin/zones", methods=["POST"])
def admin_create_zone():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    email = empty_to_none(data.get("email"))
    phone = empty_to_none(data.get("phone"))

    status = "ACTIVE" if str(data.get("status") or "Active").strip().lower() in ("active","1","true","yes") else "INACTIVE"

    digital_payment = 1 if bool(data.get("digitalPayment", True)) else 0
    cod = 1 if bool(data.get("cod", True)) else 0
    offline_payment = 1 if bool(data.get("offlinePayment", False)) else 0

    delivery_charge_active = 1 if bool(data.get("deliveryChargeActive", False)) else 0
    delivery_charge_amount = safe_float(data.get("deliveryChargeAmount"), 0.0)
    delivery_charge_message = empty_to_none(data.get("deliveryChargeMessage"))

    selected_modules = data.get("selectedModules") or []
    module_settings = data.get("moduleSettings") or []

    with engine.begin() as conn:
        # create a simple readable zone_code like Z1001...
        next_id = conn.execute(text("SELECT COALESCE(MAX(zone_id), 0) + 1 AS nid FROM zones")).mappings().first()["nid"]
        zone_code = f"Z{1000 + int(next_id)}"

        conn.execute(text("""
            INSERT INTO zones
            (zone_code, name, email, phone, status,
             digital_payment, cod, offline_payment,
             delivery_charge_active, delivery_charge_amount, delivery_charge_message,
             selected_modules_json, module_settings_json)
            VALUES
            (:code, :name, :email, :phone, :status,
             :dp, :cod, :op,
             :dca, :d_amt, :d_msg,
             :sm, :ms)
        """), {
            "code": zone_code,
            "name": name,
            "email": email,
            "phone": phone,
            "status": status,
            "dp": digital_payment,
            "cod": cod,
            "op": offline_payment,
            "dca": delivery_charge_active,
            "d_amt": delivery_charge_amount,
            "d_msg": delivery_charge_message,
            "sm": json.dumps(selected_modules),
            "ms": json.dumps(module_settings),
        })

        zid = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])
        row = conn.execute(text("SELECT * FROM zones WHERE zone_id = :id"), {"id": zid}).mappings().first()

    return jsonify(_zone_row_to_api(row)), 201


@app.route("/admin/zones/<int:zone_id>", methods=["PUT"])
def admin_update_zone(zone_id):
    data = request.json or {}

    updates = {}

    if "name" in data:
        updates["name"] = empty_to_none((data.get("name") or "").strip())
    if "email" in data:
        updates["email"] = empty_to_none((data.get("email") or "").strip())
    if "phone" in data:
        updates["phone"] = empty_to_none((data.get("phone") or "").strip())

    if "status" in data:
        updates["status"] = "ACTIVE" if str(data.get("status") or "").strip().lower() in ("active","1","true","yes") else "INACTIVE"

    if "digitalPayment" in data:
        updates["digital_payment"] = 1 if bool(data.get("digitalPayment")) else 0
    if "cod" in data:
        updates["cod"] = 1 if bool(data.get("cod")) else 0
    if "offlinePayment" in data:
        updates["offline_payment"] = 1 if bool(data.get("offlinePayment")) else 0

    if "deliveryChargeActive" in data:
        updates["delivery_charge_active"] = 1 if bool(data.get("deliveryChargeActive")) else 0
    if "deliveryChargeAmount" in data:
        updates["delivery_charge_amount"] = safe_float(data.get("deliveryChargeAmount"), 0.0)
    if "deliveryChargeMessage" in data:
        updates["delivery_charge_message"] = empty_to_none((data.get("deliveryChargeMessage") or "").strip())

    if "selectedModules" in data:
        updates["selected_modules_json"] = json.dumps(data.get("selectedModules") or [])
    if "moduleSettings" in data:
        updates["module_settings_json"] = json.dumps(data.get("moduleSettings") or [])

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["id"] = zone_id
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "id"])

    with engine.begin() as conn:
        res = conn.execute(text(f"UPDATE zones SET {set_clause} WHERE zone_id = :id"), updates)
        if res.rowcount == 0:
            return jsonify({"error": "Zone not found"}), 404

        row = conn.execute(text("""
            SELECT
              z.*,
              (SELECT COUNT(*) FROM stores s WHERE s.zone = z.name) AS stores,
              (SELECT COUNT(*) FROM riders r WHERE r.zone = z.name) AS deliverymen
            FROM zones z
            WHERE z.zone_id = :id
        """), {"id": zone_id}).mappings().first()

    return jsonify(_zone_row_to_api(row)), 200


@app.route("/admin/zones/<int:zone_id>", methods=["DELETE"])
def admin_delete_zone(zone_id):
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM zones WHERE zone_id = :id"), {"id": zone_id})
        if res.rowcount == 0:
            return jsonify({"error": "Zone not found"}), 404
    return jsonify({"ok": True}), 200


# optional helper for dropdowns
@app.route("/meta/zones", methods=["GET"])
def meta_zones():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT zone_id, name
            FROM zones
            WHERE status = 'ACTIVE'
            ORDER BY name ASC
        """)).mappings().all()
    return jsonify([{"zone_id": int(r["zone_id"]), "name": r["name"]} for r in rows]), 200

# -----------------------------
# ✅ GCS upload for notification & campaign images (NO local uploads)
# -----------------------------

# (keep BASE_DIR if used elsewhere in your file)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Keep allowed extensions (your GCS helpers use "png/jpg/jpeg/webp" without dot)
ALLOWED_IMAGE_EXT = {".png", ".jpg", ".jpeg", ".webp"}

def _image_ext_ok(filename: str) -> str:
    ext = os.path.splitext(filename or "")[1].lower()
    return ext if ext in ALLOWED_IMAGE_EXT else ""


def _save_notification_image(file_storage):
    """
    Returns: blob path like 'notifications/<uuid>.<ext>' stored in GCS
    """
    if not file_storage or not file_storage.filename:
        return None

    ext = _image_ext_ok(file_storage.filename)
    if not ext:
        raise ValueError("Only png/jpg/jpeg/webp images allowed")

    if gcs_bucket is None:
        raise ValueError("GCS not configured")

    # keep your existing naming style (uuid + ext)
    fname = f"{uuid.uuid4().hex}{ext}"
    # upload_file_to_gcs expects the file object, we want exact name -> do direct upload here
    object_path = f"notifications/{fname}"

    blob = gcs_bucket.blob(object_path)
    content_type = (
        file_storage.mimetype
        or mimetypes.guess_type(file_storage.filename)[0]
        or "application/octet-stream"
    )
    blob.upload_from_file(file_storage.stream, content_type=content_type, rewind=True)
    blob.cache_control = "public, max-age=3600"
    blob.patch()

    return object_path


def _save_campaign_image(file_storage):
    """
    Returns: blob path like 'campaigns/<uuid>.<ext>' stored in GCS
    """
    if not file_storage or not file_storage.filename:
        return None

    ext = _image_ext_ok(file_storage.filename)
    if not ext:
        raise ValueError("Only png/jpg/jpeg/webp images allowed")

    if gcs_bucket is None:
        raise ValueError("GCS not configured")

    fname = f"{uuid.uuid4().hex}{ext}"
    object_path = f"campaigns/{fname}"

    blob = gcs_bucket.blob(object_path)
    content_type = (
        file_storage.mimetype
        or mimetypes.guess_type(file_storage.filename)[0]
        or "application/octet-stream"
    )
    blob.upload_from_file(file_storage.stream, content_type=content_type, rewind=True)
    blob.cache_control = "public, max-age=3600"
    blob.patch()

    return object_path

def _notif_row_to_api(r):
    # ✅ signed URL from GCS
    img = resolve_image_url(r.get("image_path")) if r.get("image_path") else None

    return {
        "id": r["notification_id"],
        "title": r["title"],
        "description": r["description"],
        "imageUrl": img,
        "zoneId": r.get("zone_id"),
        "zoneName": r.get("zone_name"),
        "targetType": r["target_type"],   # CUSTOMERS/STORE/DELIVERYMAN
        "targetValue": json.loads(r["target_value_json"]) if r.get("target_value_json") else None,
        "status": r["status"],
        "createdAt": r["created_at"].isoformat() if r.get("created_at") else None
    }


# preflight support (important for POST/PUT multipart)
@app.route("/admin/<path:path>", methods=["OPTIONS"])
def admin_preflight(path):
    return ("", 204)

# =========================================================
#  A) DYNAMIC DROPDOWNS
# =========================================================

# ✅ Stores by zone (uses stores.zone = zones.name; collation-safe)
@app.route("/admin/stores", methods=["GET"])
def admin_meta_stores_by_zone():
    zone_id = request.args.get("zone_id", type=int)
    if not zone_id:
        return jsonify([]), 200

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.store_id, s.store_name
            FROM stores s
            JOIN zones z
              ON s.zone COLLATE utf8mb4_0900_ai_ci = z.name COLLATE utf8mb4_0900_ai_ci
            WHERE z.zone_id = :zid
            ORDER BY s.store_name ASC
        """), {"zid": zone_id}).mappings().all()

    return jsonify([{"id": r["store_id"], "name": r["store_name"]} for r in rows]), 200


@app.route("/admin/items-by-store", methods=["GET"])
def admin_items_by_store():
    store_id = request.args.get("store_id", type=int)
    if not store_id:
        return jsonify([]), 200

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT menu_item_id AS item_id, name
            FROM menu_items
            WHERE store_id = :sid
              AND (status='ACTIVE' OR status='active')
            ORDER BY name ASC
        """), {"sid": store_id}).mappings().all()

    return jsonify([{"id": int(r["item_id"]), "name": r["name"]} for r in rows]), 200

# ✅ Riders by zone (riders.zone = zones.name; both unicode_ci but still safe)
@app.route("/admin/riders-by-zone", methods=["GET"])
def admin_meta_riders_by_zone():
    zone_id = request.args.get("zone_id", type=int)
    if not zone_id:
        return jsonify([]), 200

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT r.rider_id, r.name, r.phone
            FROM riders r
            JOIN zones z
              ON r.zone COLLATE utf8mb4_unicode_ci = z.name COLLATE utf8mb4_unicode_ci
            WHERE z.zone_id = :zid
            ORDER BY r.name ASC
        """), {"zid": zone_id}).mappings().all()

    return jsonify([{"id": r["rider_id"], "name": r["name"], "phone": r["phone"]} for r in rows]), 200


# ✅ Customers dropdown (optional)
# NOTE: Your customers table in signup doesn't store zone; so this returns all customers.
@app.route("/admin/customers", methods=["GET"])
def admin_meta_customers():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT customer_id, name, phone
            FROM customers
            ORDER BY customer_id DESC
            LIMIT 500
        """)).mappings().all()

    return jsonify([{"id": r["customer_id"], "name": r.get("name"), "phone": r["phone"]} for r in rows]), 200


# =========================================================
#  B) NOTIFICATIONS CRUD
# =========================================================

@app.route("/admin/notifications", methods=["GET"])
def admin_list_notifications():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            ORDER BY n.notification_id DESC
        """)).mappings().all()

    return jsonify([_notif_row_to_api(r) for r in rows]), 200


# multipart POST: title, description, zoneId, targetType, targetValueJson, image
@app.route("/admin/notifications", methods=["POST"])
def admin_create_notification():
    title = (request.form.get("title") or "").strip()
    description = (request.form.get("description") or "").strip()
    target_type = (request.form.get("targetType") or "CUSTOMERS").strip().upper()
    zone_id = request.form.get("zoneId", type=int)

    if not title or not description:
        return jsonify({"error": "title & description required"}), 400

    if target_type not in ("CUSTOMERS", "STORE", "DELIVERYMAN"):
        return jsonify({"error": "Invalid targetType"}), 400

    # targetValueJson is optional JSON array (e.g. [12,13] or ["9999.."])
    target_value_json = request.form.get("targetValueJson")
    if target_value_json:
        try:
            json.loads(target_value_json)
        except:
            return jsonify({"error": "targetValueJson must be valid JSON"}), 400

    img_path = None
    f = request.files.get("image")
    if f and f.filename:
        try:
            img_path = _save_notification_image(f)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO notifications (zone_id, title, description, image_path, target_type, target_value_json, status)
            VALUES (:zone_id, :title, :description, :image_path, :target_type, :target_value_json, 'SENT')
        """), {
            "zone_id": zone_id,
            "title": title,
            "description": description,
            "image_path": img_path,
            "target_type": target_type,
            "target_value_json": target_value_json
        })

        new_id = conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"]

        row = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            WHERE n.notification_id = :id
        """), {"id": new_id}).mappings().first()

    return jsonify(_notif_row_to_api(row)), 201


@app.route("/admin/notifications/<int:notification_id>", methods=["PUT"])
def admin_update_notification(notification_id):
    title = (request.form.get("title") or "").strip()
    description = (request.form.get("description") or "").strip()
    target_type = (request.form.get("targetType") or "").strip().upper()
    zone_id = request.form.get("zoneId", type=int)
    target_value_json = request.form.get("targetValueJson")

    updates = {}
    if title: updates["title"] = title
    if description: updates["description"] = description
    if zone_id is not None: updates["zone_id"] = zone_id

    if target_type:
        if target_type not in ("CUSTOMERS", "STORE", "DELIVERYMAN"):
            return jsonify({"error": "Invalid targetType"}), 400
        updates["target_type"] = target_type

    if target_value_json is not None:
        if target_value_json == "":
            updates["target_value_json"] = None
        else:
            try:
                json.loads(target_value_json)
                updates["target_value_json"] = target_value_json
            except:
                return jsonify({"error": "targetValueJson must be valid JSON"}), 400

    f = request.files.get("image")
    if f and f.filename:
        try:
            updates["image_path"] = _save_notification_image(f)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    set_sql = ", ".join([f"{k} = :{k}" for k in updates.keys()])

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT notification_id FROM notifications WHERE notification_id=:id"),
            {"id": notification_id},
        ).first()
        if not exists:
            return jsonify({"error": "Notification not found"}), 404

        conn.execute(text(f"""
            UPDATE notifications
            SET {set_sql}
            WHERE notification_id = :id
        """), {**updates, "id": notification_id})

        row = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            WHERE n.notification_id = :id
        """), {"id": notification_id}).mappings().first()

    return jsonify(_notif_row_to_api(row)), 200


@app.route("/admin/notifications/<int:notification_id>", methods=["DELETE"])
def admin_delete_notification(notification_id):
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT image_path FROM notifications WHERE notification_id=:id
        """), {"id": notification_id}).mappings().first()

        if not row:
            return jsonify({"error": "Notification not found"}), 404

        # delete db row first (same as your old flow)
        conn.execute(
            text("DELETE FROM notifications WHERE notification_id=:id"),
            {"id": notification_id},
        )

    # ✅ delete image from GCS (best-effort)
    try:
        img_path = (row.get("image_path") or "").strip().lstrip("/")
        if img_path and gcs_bucket is not None:
            gcs_bucket.blob(img_path).delete()
    except Exception as e:
        # don't fail delete if image delete fails
        print("⚠️ GCS delete failed:", e)

    return jsonify({"success": True}), 200

@app.route("/customer/notifications", methods=["GET"])
def customer_list_notifications():
    zone_id = request.args.get("zoneId", type=int)
    store_id = request.args.get("storeId", type=int)  # optional (if you want store-targeted notifications too)
    limit = request.args.get("limit", default=50, type=int)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            WHERE n.status = 'SENT'
              AND (
                    LOWER(COALESCE(n.target, '')) = 'customers'
                    OR UPPER(COALESCE(n.target_type, '')) = 'CUSTOMERS'
                  )
              AND (
                    :zone_id IS NULL
                    OR n.zone_id IS NULL
                    OR n.zone_id = :zone_id
                  )
              AND (
                    :store_id IS NULL
                    OR n.target_value_json IS NULL
                    OR JSON_EXTRACT(n.target_value_json, '$.targetId') IS NULL
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(n.target_value_json, '$.targetId')) AS SIGNED) = :store_id
                  )
            ORDER BY n.notification_id DESC
            LIMIT :lim
        """), {"zone_id": zone_id, "store_id": store_id, "lim": limit}).mappings().all()

    return jsonify([_notif_row_to_api(r) for r in rows]), 200

@app.route("/admin/flash-sales", methods=["GET"])
def get_flash_sales():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              flash_sale_id,
              title,
              start_date,
              end_date,
              publish
            FROM flash_sales
            ORDER BY flash_sale_id DESC
        """)).mappings().all()

    result = []
    for r in rows:
        start = r["start_date"]
        end = r["end_date"]

        result.append({
            "id": r["flash_sale_id"],
            "title": r["title"],
            "startDate": str(start),  # safe for JSON
            "endDate": str(end),
            "duration": f"{str(start)[:10]} to {str(end)[:10]}",
            "publish": r["publish"] or "Yes",
        })

    return jsonify(result), 200

@app.route("/admin/flash-sales", methods=["POST"])
def create_flash_sale():
    data = request.json

    title = data.get("title")
    start_date = data.get("startDate")
    end_date = data.get("endDate")

    if not title or not start_date or not end_date:
        return jsonify({"error": "Missing required fields"}), 400

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO flash_sales (title, start_date, end_date)
            VALUES (:title, :start_date, :end_date)
        """), {
            "title": title,
            "start_date": start_date,
            "end_date": end_date
        })

    return jsonify({"message": "Flash Sale created"}), 201

@app.route("/admin/flash-sales/<int:flash_id>", methods=["PUT"])
def update_flash_sale(flash_id):
    data = request.json

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE flash_sales
            SET title = :title,
                start_date = :start_date,
                end_date = :end_date
            WHERE flash_sale_id = :id
        """), {
            "title": data.get("title"),
            "start_date": data.get("startDate"),
            "end_date": data.get("endDate"),
            "id": flash_id
        })

    return jsonify({"message": "Flash Sale updated"}), 200

@app.route("/admin/flash-sales/<int:flash_id>", methods=["DELETE"])
def delete_flash_sale(flash_id):
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM flash_sales WHERE flash_sale_id = :id
        """), {"id": flash_id})

    return jsonify({"message": "Flash Sale deleted"}), 200

@app.route("/admin/flash-sales/<int:flash_id>/toggle", methods=["PATCH"])
def toggle_flash_sale(flash_id):
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT publish FROM flash_sales WHERE flash_sale_id=:id
        """), {"id": flash_id}).mappings().first()

        if not row:
            return jsonify({"error": "Not found"}), 404

        new_status = "No" if row["publish"] == "Yes" else "Yes"

        conn.execute(text("""
            UPDATE flash_sales SET publish=:status WHERE flash_sale_id=:id
        """), {"status": new_status, "id": flash_id})

    return jsonify({"publish": new_status}), 200

# customer signup
@app.route("/customer/signup", methods=["POST"])
def customer_signup():
    data = request.json or {}
    name = str(data.get("name") or "").strip()
    phone = str(data.get("phone") or "").strip()
    email = str(data.get("email") or "").strip()
    password = str(data.get("password") or "").strip()

    if not phone or not password:
        return jsonify({"error": "phone and password required"}), 400

    pw_hash = bcrypt_hash_password(password)

    with engine.begin() as conn:
        cust = conn.execute(text("""
            SELECT customer_id FROM customers WHERE phone=:ph LIMIT 1
        """), {"ph": phone}).mappings().first()

        if cust:
            customer_id = int(cust["customer_id"])
            # ✅ update name/email if provided
            conn.execute(text("""
                UPDATE customers
                SET name = COALESCE(:name, name),
                    email = COALESCE(:email, email)
                WHERE customer_id = :cid
            """), {"name": name or None, "email": email or None, "cid": customer_id})
        else:
            conn.execute(text("""
                INSERT INTO customers (name, phone, email, status)
                VALUES (:name, :ph, :email, 'ACTIVE')
            """), {"name": name or None, "ph": phone, "email": email or None})
            customer_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        exists = conn.execute(text("""
            SELECT phone FROM customer_auth WHERE phone=:ph LIMIT 1
        """), {"ph": phone}).mappings().first()
        if exists:
            return jsonify({"error": "Account already exists"}), 409

        conn.execute(text("""
            INSERT INTO customer_auth (customer_id, phone, password_hash, status)
            VALUES (:cid, :ph, :pwh, 'ACTIVE')
        """), {"cid": customer_id, "ph": phone, "pwh": pw_hash})

    return jsonify({"ok": True, "customer_id": customer_id}), 201

#customer login
@app.route("/customer/login", methods=["POST"])
def customer_login():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    password = str(data.get("password") or "").strip()

    if not phone or not password:
        return jsonify({"error": "phone and password required"}), 400

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT ca.customer_id, ca.password_hash, ca.status, c.name
            FROM customer_auth ca
            JOIN customers c ON c.customer_id = ca.customer_id
            WHERE ca.phone=:ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not row:
        return jsonify({"error": "Invalid credentials"}), 401

    if str(row.get("status") or "") != "ACTIVE":
        return jsonify({"error": "Customer blocked"}), 403

    if not bcrypt_check_password(password, str(row.get("password_hash") or "")):
        return jsonify({"error": "Invalid credentials"}), 401

    return jsonify({
        "ok": True,
        "customer": {
            "customer_id": int(row["customer_id"]),
            "name": row.get("name") or "",
            "phone": phone
        }
    }), 200

# forgot password
@app.route("/customer/forgot-password", methods=["POST"])
def customer_forgot_password():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400

    code = str(secrets.randbelow(900000) + 100000)  # 6-digit
    code_hash = bcrypt_hash_password(code)
    expires = datetime.utcnow() + timedelta(minutes=10)

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT customer_id FROM customer_auth WHERE phone=:ph LIMIT 1
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"ok": True}), 200  # don't reveal

        conn.execute(text("""
            UPDATE customer_auth
            SET reset_code_hash=:h, reset_code_expires_at=:exp
            WHERE phone=:ph
        """), {"h": code_hash, "exp": expires, "ph": phone})

    # TODO: send via SMS provider
    return jsonify({"ok": True, "test_code": code}), 200  # remove in production

# request reset password
@app.route("/customer/request-reset", methods=["POST"])
def request_reset():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400

    code = str(secrets.randbelow(900000) + 100000)
    code_hash = bcrypt_hash_password(code)
    expires = datetime.utcnow() + timedelta(minutes=10)

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT customer_id FROM customer_auth WHERE phone=:ph
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"ok": True}), 200

        conn.execute(text("""
            UPDATE customer_auth
            SET reset_code_hash=:h,
                reset_code_expires_at=:exp
            WHERE phone=:ph
        """), {"h": code_hash, "exp": expires, "ph": phone})

    print("RESET CODE:", code)  # 🔥 only for development
    return jsonify({"ok": True}), 200

# reset password
@app.route("/customer/reset-password", methods=["POST"])
def customer_reset_password():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    code = str(data.get("code") or "").strip()
    new_password = str(data.get("new_password") or "").strip()

    if not phone or not code or not new_password:
        return jsonify({"error": "phone, code, new_password required"}), 400

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT reset_code_hash, reset_code_expires_at
            FROM customer_auth
            WHERE phone=:ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"error": "Invalid request"}), 400

        exp = row.get("reset_code_expires_at")
        if not exp or datetime.utcnow() > exp:
            return jsonify({"error": "Code expired"}), 400

        if not bcrypt_check_password(code, str(row.get("reset_code_hash") or "")):
            return jsonify({"error": "Invalid code"}), 400

        new_hash = bcrypt_hash_password(new_password)
        conn.execute(text("""
            UPDATE customer_auth
            SET password_hash=:ph,
                reset_code_hash=NULL,
                reset_code_expires_at=NULL
            WHERE phone=:phone
        """), {"ph": new_hash, "phone": phone})

    return jsonify({"ok": True}), 200

# ======================
# CUSTOMER RESET (NO OTP) - Prefill + Update
# ======================

@app.route("/customer/prefill", methods=["GET"])
def customer_prefill():
    phone = str(request.args.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT customer_id, name, phone, email, status
            FROM customers
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not row:
        return jsonify({"error": "Customer not found"}), 404

    return jsonify({
        "ok": True,
        "customer": {
            "customer_id": int(row["customer_id"]),
            "name": row.get("name") or "",
            "phone": row.get("phone") or "",
            "email": row.get("email") or ""
        }
    }), 200


@app.route("/customer/reset-password-no-otp", methods=["POST"])
def reset_password_no_otp():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    email = str(data.get("email") or "").strip()
    new_password = str(data.get("new_password") or "").strip()

    if not phone or not new_password:
        return jsonify({"error": "phone and new_password required"}), 400

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT ca.customer_id, c.email
            FROM customer_auth ca
            JOIN customers c ON c.customer_id = ca.customer_id
            WHERE ca.phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"error": "Customer not found"}), 404

        db_email = (row.get("email") or "").strip().lower()
        if db_email and email and db_email != email.lower():
            return jsonify({"error": "Email does not match"}), 400

        new_hash = bcrypt_hash_password(new_password)

        res = conn.execute(text("""
            UPDATE customer_auth
            SET password_hash = :h
            WHERE phone = :ph
        """), {"h": new_hash, "ph": phone})   # ✅ IMPORTANT FIX

        if res.rowcount == 0:
            return jsonify({"error": "Auth row not found"}), 404

    return jsonify({"ok": True}), 200

# ======================
# VENDOR LOGIN (use vendors table)
# Keep old route name /store-login for compatibility
# ======================
@app.route("/store-login", methods=["POST"])
def store_login():
    data = request.json or {}
    email = str(data.get("email", "") or "").strip()
    password = str(data.get("password", "") or "").strip()

    if not email or not password:
        return jsonify({"error": "email and password required"}), 400

    with engine.connect() as conn:
        v = conn.execute(text("""
            SELECT vendor_id, name, email, phone, password_hash, status
            FROM vendors
            WHERE email = :email
            LIMIT 1
        """), {"email": email}).mappings().first()

    if not v:
        return jsonify({"error": "Invalid Credentials"}), 401

    # NOTE: currently comparing plain string for your testing
    if str(v.get("password_hash") or "") != password:
        return jsonify({"error": "Invalid Credentials"}), 401

    if str(v.get("status")) != "ACTIVE":
        return jsonify({"error": "Vendor blocked"}), 403

    # return vendor + stores list
    with engine.connect() as conn:
        stores = conn.execute(text("""
            SELECT * FROM stores
            WHERE vendor_id = :vid
            ORDER BY store_id DESC
        """), {"vid": int(v["vendor_id"])}).mappings().all()

    out_stores = []
    for s in stores:
        d = dict(s)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["cover_url"] = resolve_image_url(d.get("cover_url"))
        d["featured"] = int(d.get("is_featured") or 0)
        d["address"] = " ".join([x for x in [d.get("address_line1"), d.get("address_line2"), d.get("city"), d.get("state"), d.get("pincode")] if x])
        out_stores.append(d)

    return jsonify({
        "vendor": {
            "vendor_id": int(v["vendor_id"]),
            "name": v.get("name"),
            "email": v.get("email"),
            "phone": v.get("phone"),
        },
        "stores": out_stores
    }), 200


# ======================
# MENU SECTION HELPERS
# ======================
def get_or_create_section(conn, store_id: int, section_name: str):
    name = (section_name or "").strip()
    if not name:
        return None

    row = conn.execute(text("""
        SELECT section_id
        FROM menu_sections
        WHERE store_id = :sid AND name = :name
        LIMIT 1
    """), {"sid": store_id, "name": name}).mappings().first()

    if row:
        return int(row["section_id"])

    conn.execute(text("""
        INSERT INTO menu_sections (store_id, name, sort_order, status)
        VALUES (:sid, :name, 0, 'ACTIVE')
    """), {"sid": store_id, "name": name})

    sec_id = conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"]
    return int(sec_id)


def get_active_variant_price(conn, menu_item_id: int):
    row = conn.execute(text("""
        SELECT variant_id, variant_name, price
        FROM menu_item_variants
        WHERE menu_item_id = :mid AND status = 'ACTIVE'
        ORDER BY variant_id ASC
        LIMIT 1
    """), {"mid": menu_item_id}).mappings().first()
    return row


# ======================
# MENU ITEMS ROUTES
# Keep OLD route names: /admin/add-item, /admin/edit-item/<id>, etc.
# Incoming old payload keys supported: item_name, category, price, image
# ======================
@app.route("/admin/add-item", methods=["POST"])
def admin_add_item():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    store_id = safe_int(data.get("store_id", 0), 0)
    if store_id == 0:
        return jsonify({"error": "Invalid store_id"}), 400

    item_name = str(data.get("item_name") or data.get("name") or "").strip()
    if not item_name:
        return jsonify({"error": "item_name is required"}), 400

    # old "category" -> section name
    section_name = empty_to_none(data.get("category")) or empty_to_none(data.get("section_name"))

    # image
    image_url_value = empty_to_none(data.get("image_url"))
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                image_url_value = upload_file_to_gcs(f, folder=f"menu_items/{store_id}")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    price = safe_float(data.get("price"), 0.0)
    variant_name = str(data.get("variant_name") or "Regular").strip() or "Regular"

    is_veg = 1 if str(data.get("is_veg") or "").strip().lower() in ("1", "true", "yes") else 0
    is_egg = 1 if str(data.get("is_egg") or "").strip().lower() in ("1", "true", "yes") else 0

    with engine.begin() as conn:
        # ensure store exists
        st = conn.execute(text("SELECT store_id FROM stores WHERE store_id = :sid"), {"sid": store_id}).mappings().first()
        if not st:
            return jsonify({"error": "Store not found"}), 404

        section_id = get_or_create_section(conn, store_id, section_name) if section_name else None

        conn.execute(text("""
            INSERT INTO menu_items
            (store_id, section_id, name, description_short, image_url, is_veg, is_egg, status, sort_order)
            VALUES
            (:store_id, :section_id, :name, :desc, :image_url, :is_veg, :is_egg, 'ACTIVE', 0)
        """), {
            "store_id": store_id,
            "section_id": section_id,
            "name": item_name,
            "desc": empty_to_none(data.get("short_description")),
            "image_url": empty_to_none(image_url_value),
            "is_veg": is_veg,
            "is_egg": is_egg,
        })

        menu_item_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        # default variant (price)
        conn.execute(text("""
            INSERT INTO menu_item_variants (menu_item_id, variant_name, price, status)
            VALUES (:mid, :vname, :price, 'ACTIVE')
        """), {"mid": menu_item_id, "vname": variant_name, "price": price})

    return jsonify({
        "message": "Item Added Successfully",
        "menu_item_id": menu_item_id,
        "image_url": resolve_image_url(image_url_value) if image_url_value else ""
    }), 201


@app.route("/admin/edit-item/<int:menu_item_id>", methods=["PUT"])
def admin_edit_item(menu_item_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    updates_item = {}
    updates_variant = {}

    if "item_name" in data or "name" in data:
        updates_item["name"] = empty_to_none(str(data.get("item_name") or data.get("name") or "").strip())

    if "short_description" in data:
        updates_item["description_short"] = empty_to_none(str(data.get("short_description") or "").strip())

    if "category" in data or "section_name" in data:
        # will map to section_id
        pass

    if "is_veg" in data:
        updates_item["is_veg"] = 1 if str(data.get("is_veg") or "").strip().lower() in ("1", "true", "yes") else 0
    if "is_egg" in data:
        updates_item["is_egg"] = 1 if str(data.get("is_egg") or "").strip().lower() in ("1", "true", "yes") else 0

    if "status" in data:
        st = str(data.get("status") or "").strip().lower()
        # accept old 'active/inactive'
        if st in ("active", "1", "true", "yes"):
            updates_item["status"] = "ACTIVE"
        elif st in ("out_of_stock", "outofstock"):
            updates_item["status"] = "OUT_OF_STOCK"
        else:
            updates_item["status"] = "INACTIVE"

    # price/variant updates (update first ACTIVE variant)
    if "price" in data:
        updates_variant["price"] = safe_float(data.get("price"), 0.0)
    if "variant_name" in data:
        updates_variant["variant_name"] = empty_to_none(str(data.get("variant_name") or "").strip())

    # image logic
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                with engine.connect() as conn:
                    row = conn.execute(
                        text("SELECT store_id FROM menu_items WHERE menu_item_id = :mid"),
                        {"mid": menu_item_id}
                    ).mappings().first()
                if not row:
                    return jsonify({"error": "Item Not Found"}), 404
                store_id = int(row["store_id"])
                gcs_path = upload_file_to_gcs(f, folder=f"menu_items/{store_id}")
                updates_item["image_url"] = gcs_path
        else:
            # explicit image_url
            if "image_url" in data:
                updates_item["image_url"] = empty_to_none(str(data.get("image_url") or "").strip())
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    with engine.begin() as conn:
        base = conn.execute(text("""
            SELECT store_id FROM menu_items WHERE menu_item_id = :mid
        """), {"mid": menu_item_id}).mappings().first()
        if not base:
            return jsonify({"error": "Item Not Found"}), 404

        store_id = int(base["store_id"])

        # category -> section
        if "category" in data or "section_name" in data:
            section_name = empty_to_none(data.get("category")) or empty_to_none(data.get("section_name"))
            if section_name:
                section_id = get_or_create_section(conn, store_id, section_name)
                updates_item["section_id"] = section_id

        # update menu_items
        if updates_item:
            updates_item["mid"] = menu_item_id
            set_clause = ", ".join([f"{k} = :{k}" for k in updates_item.keys() if k != "mid"])
            conn.execute(
                text(f"UPDATE menu_items SET {set_clause} WHERE menu_item_id = :mid"),
                updates_item
            )

        # update variant (first ACTIVE)
        if updates_variant:
            # find active variant
            v = conn.execute(text("""
                SELECT variant_id
                FROM menu_item_variants
                WHERE menu_item_id = :mid AND status = 'ACTIVE'
                ORDER BY variant_id ASC
                LIMIT 1
            """), {"mid": menu_item_id}).mappings().first()

            if v:
                updates_variant["vid"] = int(v["variant_id"])
                setv = ", ".join([f"{k} = :{k}" for k in updates_variant.keys() if k != "vid"])
                conn.execute(
                    text(f"UPDATE menu_item_variants SET {setv} WHERE variant_id = :vid"),
                    updates_variant
                )

    return jsonify({
        "message": "Item Updated",
        "image_url": resolve_image_url(updates_item.get("image_url")) if updates_item.get("image_url") else ""
    }), 200


@app.route("/admin/delete-item/<int:menu_item_id>", methods=["DELETE"])
def admin_delete_item(menu_item_id):
    with engine.begin() as conn:
        # delete variants first (FK)
        conn.execute(text("DELETE FROM menu_item_variants WHERE menu_item_id = :mid"), {"mid": menu_item_id})
        res = conn.execute(text("DELETE FROM menu_items WHERE menu_item_id = :mid"), {"mid": menu_item_id})
        if res.rowcount == 0:
            return jsonify({"error": "Item Not Found"}), 404
    return jsonify({"message": "Item Deleted"}), 200


@app.route("/get-items/<int:store_id>", methods=["GET"])
def get_items(store_id):
    """
    Returns old-style keys for compatibility:
      item_name, category, price, image_url ...

    ✅ NEW FILTERS:
      /get-items/3?is_veg=1   -> only veg
      /get-items/3?is_egg=1   -> only egg items
    """
    veg_q = request.args.get("is_veg", None)
    egg_q = request.args.get("is_egg", None)

    where = ["mi.store_id = :sid"]
    params = {"sid": store_id}

    if veg_q is not None and str(veg_q).strip() != "":
        where.append("mi.is_veg = :veg")
        params["veg"] = parse_bool_int(veg_q, 0)

    if egg_q is not None and str(egg_q).strip() != "":
        where.append("mi.is_egg = :egg")
        params["egg"] = parse_bool_int(egg_q, 0)

    where_sql = "WHERE " + " AND ".join(where)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              mi.menu_item_id,
              mi.store_id,
              mi.name,
              mi.description_short,
              mi.image_url,
              mi.status,
              mi.is_veg,
              mi.is_egg,
              ms.name AS section_name
            FROM menu_items mi
            LEFT JOIN menu_sections ms ON ms.section_id = mi.section_id
            {where_sql}
            ORDER BY mi.menu_item_id DESC
        """), params).mappings().all()

        out = []
        for r in rows:
            rr = dict(r)
            price_row = get_active_variant_price(conn, int(rr["menu_item_id"]))
            price = float(price_row["price"]) if price_row else 0.0
            variant_name = price_row["variant_name"] if price_row else "Regular"

            out.append({
                "menu_item_id": int(rr["menu_item_id"]),
                "store_id": int(rr["store_id"]),
                "item_name": rr.get("name") or "",
                "short_description": rr.get("description_short") or "",
                "image_url": resolve_image_url(rr.get("image_url")),
                "category": rr.get("section_name") or "",
                "price": price,
                "variant_name": variant_name,
                "status": "active" if str(rr.get("status")) == "ACTIVE" else "inactive",

                # ✅ important for app filter + edit prefill
                "is_veg": int(rr.get("is_veg") or 0),
                "is_egg": int(rr.get("is_egg") or 0),
            })

    return jsonify(out), 200

@app.route("/admin/menu-items", methods=["GET"])
def admin_menu_items_list():
    store_id = safe_int(request.args.get("store_id", 0), 0)
    section = (request.args.get("category") or "").strip()  # keep old param name category

    # ✅ optional filters
    veg_q = request.args.get("is_veg", None)
    egg_q = request.args.get("is_egg", None)

    where = []
    params = {}

    if store_id:
        where.append("mi.store_id = :sid")
        params["sid"] = store_id

    if section:
        where.append("ms.name = :sec")
        params["sec"] = section

    if veg_q is not None and str(veg_q).strip() != "":
        where.append("mi.is_veg = :veg")
        params["veg"] = parse_bool_int(veg_q, 0)

    if egg_q is not None and str(egg_q).strip() != "":
        where.append("mi.is_egg = :egg")
        params["egg"] = parse_bool_int(egg_q, 0)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              mi.menu_item_id,
              mi.store_id,
              mi.name,
              mi.description_short,
              mi.image_url,
              mi.status,
              mi.is_veg,
              mi.is_egg,
              ms.name AS section_name,
              s.store_name
            FROM menu_items mi
            LEFT JOIN menu_sections ms ON ms.section_id = mi.section_id
            LEFT JOIN stores s ON s.store_id = mi.store_id
            {where_sql}
            ORDER BY mi.menu_item_id DESC
        """), params).mappings().all()

        out = []
        for r in rows:
            rr = dict(r)
            price_row = get_active_variant_price(conn, int(rr["menu_item_id"]))
            rr["price"] = float(price_row["price"]) if price_row else 0.0
            rr["variant_name"] = price_row["variant_name"] if price_row else "Regular"
            rr["image_url"] = resolve_image_url(rr.get("image_url"))

            # compatibility keys (your old logic)
            rr["item_name"] = rr.pop("name")
            rr["short_description"] = rr.pop("description_short")
            rr["category"] = rr.pop("section_name") or ""

            # ✅ ensure returned
            rr["is_veg"] = int(rr.get("is_veg") or 0)
            rr["is_egg"] = int(rr.get("is_egg") or 0)

            out.append(rr)

    return jsonify(out), 200

@app.route("/meta/categories", methods=["GET"])
def meta_categories():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT category_id, name
            FROM categories
            WHERE status IN ('active','ACTIVE')
            ORDER BY name ASC
        """)).mappings().all()

    return jsonify([
        {"category_id": int(r["category_id"]), "name": r["name"]}
        for r in rows
    ]), 200

@app.route("/admin/menu-sections", methods=["GET"])
def admin_menu_sections_list():
    store_id = safe_int(request.args.get("store_id", 0), 0)
    if not store_id:
        return jsonify({"error": "store_id is required"}), 400

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT section_id, name, status, sort_order
            FROM menu_sections
            WHERE store_id = :sid
            ORDER BY sort_order ASC, name ASC
        """), {"sid": store_id}).mappings().all()

    return jsonify([dict(r) for r in rows]), 200


@app.route("/admin/menu-sections", methods=["POST"])
def admin_menu_sections_create():
    data = request.json or {}
    store_id = safe_int(data.get("store_id", 0), 0)
    name = (data.get("name") or "").strip()

    if not store_id or not name:
        return jsonify({"error": "store_id and name are required"}), 400

    with engine.begin() as conn:
        # avoid duplicates
        exists = conn.execute(text("""
            SELECT section_id FROM menu_sections
            WHERE store_id = :sid AND name = :name
            LIMIT 1
        """), {"sid": store_id, "name": name}).mappings().first()

        if exists:
            return jsonify({"error": "Section already exists"}), 400

        conn.execute(text("""
            INSERT INTO menu_sections (store_id, name, sort_order, status)
            VALUES (:sid, :name, 0, 'ACTIVE')
        """), {"sid": store_id, "name": name})

    return jsonify({"ok": True}), 201

# ======================
# ✅ CATEGORIES + UNITS (ADMIN MASTER DATA)
# Needed because your React Admin panel uses these endpoints.
# Restaurant-only menu still uses menu_sections per store.
# ======================

@app.route("/admin/categories", methods=["GET"])
def admin_get_categories():
    system_module = (request.args.get("system_module") or "").strip()

    sql = """
        SELECT category_id, name, system_module, status, featured, priority, image_url
        FROM categories
    """
    params = {}
    if system_module:
        sql += " WHERE system_module = :sm"
        params["sm"] = system_module
    sql += " ORDER BY priority ASC, category_id DESC"

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["image_url"] = resolve_image_url(d.get("image_url"))
        out.append(d)

    return jsonify(out), 200


@app.route("/admin/categories", methods=["POST"])
def admin_create_category():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    system_module = (data.get("system_module") or "Restaurant").strip()
    if system_module not in ("Grocery", "Restaurant"):
        system_module = "Restaurant"

    status = (data.get("status") or "active").strip().lower()
    status = "inactive" if status == "inactive" else "active"

    featured = 1 if str(data.get("featured") or "0").strip() in ("1", "true", "yes") else 0
    priority = safe_int(data.get("priority", 0), 0)

    image_url_value = empty_to_none(data.get("image_url"))

    # upload image if provided
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                image_url_value = upload_file_to_gcs(f, folder="categories")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO categories (name, system_module, status, featured, priority, image_url)
            VALUES (:name, :sm, :st, :feat, :prio, :img)
        """), {
            "name": name,
            "sm": system_module,
            "st": status,
            "feat": featured,
            "prio": priority,
            "img": image_url_value
        })

        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])
        row = conn.execute(text("""
            SELECT category_id, name, system_module, status, featured, priority, image_url
            FROM categories WHERE category_id = :id
        """), {"id": new_id}).mappings().first()

    d = dict(row)
    d["image_url"] = resolve_image_url(d.get("image_url"))
    return jsonify(d), 201


@app.route("/admin/categories/<int:category_id>", methods=["PUT"])
def admin_update_category(category_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    updates = {}

    if "name" in data:
        updates["name"] = empty_to_none((data.get("name") or "").strip())
    if "system_module" in data:
        sm = (data.get("system_module") or "").strip()
        if sm in ("Grocery", "Restaurant"):
            updates["system_module"] = sm
    if "status" in data:
        st = (data.get("status") or "").strip().lower()
        updates["status"] = "inactive" if st == "inactive" else "active"
    if "featured" in data:
        updates["featured"] = 1 if str(data.get("featured") or "0").strip() in ("1", "true", "yes") else 0
    if "priority" in data:
        updates["priority"] = safe_int(data.get("priority", 0), 0)

    # image upload
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                updates["image_url"] = upload_file_to_gcs(f, folder=f"categories/{category_id}")
        else:
            if "image_url" in data:
                updates["image_url"] = empty_to_none((data.get("image_url") or "").strip())
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["id"] = category_id
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "id"])

    with engine.begin() as conn:
        res = conn.execute(
            text(f"UPDATE categories SET {set_clause} WHERE category_id = :id"),
            updates
        )
        if res.rowcount == 0:
            return jsonify({"error": "Category not found"}), 404

        row = conn.execute(text("""
            SELECT category_id, name, system_module, status, featured, priority, image_url
            FROM categories WHERE category_id = :id
        """), {"id": category_id}).mappings().first()

    d = dict(row)
    d["image_url"] = resolve_image_url(d.get("image_url"))
    return jsonify(d), 200


@app.route("/admin/categories/<int:category_id>", methods=["DELETE"])
def admin_delete_category(category_id):
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM categories WHERE category_id = :id"), {"id": category_id})
        if res.rowcount == 0:
            return jsonify({"error": "Category not found"}), 404
    return jsonify({"ok": True}), 200

# ======================
# SUB CATEGORIES (ADMIN)
# ======================

@app.route("/admin/subcategories", methods=["GET"])
def admin_get_subcategories():
    """
    Returns rows exactly like React expects:
    sub_category_id, category_id, category_name, name, status, featured, priority
    """
    category_id = safe_int_or_none(request.args.get("category_id"))
    system_module = (request.args.get("system_module") or "").strip()

    sql = """
        SELECT
          sc.sub_category_id,
          sc.category_id,
          c.name AS category_name,
          sc.name,
          sc.status,
          sc.featured,
          sc.priority
        FROM sub_categories sc
        JOIN categories c ON c.category_id = sc.category_id
    """
    where = []
    params = {}

    if category_id:
        where.append("sc.category_id = :cid")
        params["cid"] = category_id

    if system_module:
        where.append("c.system_module = :sm")
        params["sm"] = system_module

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY sc.priority ASC, sc.sub_category_id DESC"

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return jsonify([dict(r) for r in rows]), 200


@app.route("/admin/subcategories", methods=["POST"])
def admin_create_subcategory():
    data = request.json or {}

    category_id = safe_int_or_none(data.get("category_id"))
    name = (data.get("name") or "").strip()

    if not category_id or not name:
        return jsonify({"error": "category_id and name are required"}), 400

    status = (data.get("status") or "active").strip().lower()
    status = "inactive" if status == "inactive" else "active"

    featured = 1 if str(data.get("featured") or "0").strip() in ("1", "true", "yes") else 0
    priority = safe_int(data.get("priority", 0), 0)

    with engine.begin() as conn:
        # ensure category exists
        cat = conn.execute(
            text("SELECT category_id FROM categories WHERE category_id = :cid LIMIT 1"),
            {"cid": category_id}
        ).mappings().first()
        if not cat:
            return jsonify({"error": "Category not found"}), 404

        # prevent duplicates
        dup = conn.execute(text("""
            SELECT sub_category_id
            FROM sub_categories
            WHERE category_id = :cid AND name = :name
            LIMIT 1
        """), {"cid": category_id, "name": name}).mappings().first()
        if dup:
            return jsonify({"error": "Sub Category already exists for this Main Category"}), 400

        conn.execute(text("""
            INSERT INTO sub_categories (category_id, name, status, featured, priority)
            VALUES (:cid, :name, :st, :feat, :prio)
        """), {
            "cid": category_id,
            "name": name,
            "st": status,
            "feat": featured,
            "prio": priority
        })

        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        row = conn.execute(text("""
            SELECT
              sc.sub_category_id,
              sc.category_id,
              c.name AS category_name,
              sc.name,
              sc.status,
              sc.featured,
              sc.priority
            FROM sub_categories sc
            JOIN categories c ON c.category_id = sc.category_id
            WHERE sc.sub_category_id = :id
        """), {"id": new_id}).mappings().first()

    return jsonify(dict(row)), 201


@app.route("/admin/subcategories/<int:sub_category_id>", methods=["PUT"])
def admin_update_subcategory(sub_category_id):
    data = request.json or {}

    updates = {}

    if "category_id" in data:
        updates["category_id"] = safe_int_or_none(data.get("category_id"))
    if "name" in data:
        updates["name"] = (data.get("name") or "").strip()
    if "status" in data:
        st = (data.get("status") or "").strip().lower()
        updates["status"] = "inactive" if st == "inactive" else "active"
    if "featured" in data:
        updates["featured"] = 1 if str(data.get("featured") or "0").strip() in ("1", "true", "yes") else 0
    if "priority" in data:
        updates["priority"] = safe_int(data.get("priority", 0), 0)

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["id"] = sub_category_id
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "id"])

    with engine.begin() as conn:
        # ensure exists
        exists = conn.execute(
            text("SELECT sub_category_id FROM sub_categories WHERE sub_category_id = :id"),
            {"id": sub_category_id}
        ).first()
        if not exists:
            return jsonify({"error": "Sub Category not found"}), 404

        # if category_id changed, ensure category exists
        if updates.get("category_id"):
            cat = conn.execute(
                text("SELECT category_id FROM categories WHERE category_id = :cid LIMIT 1"),
                {"cid": updates["category_id"]}
            ).first()
            if not cat:
                return jsonify({"error": "Category not found"}), 404

        conn.execute(text(f"""
            UPDATE sub_categories
            SET {set_clause}
            WHERE sub_category_id = :id
        """), updates)

        row = conn.execute(text("""
            SELECT
              sc.sub_category_id,
              sc.category_id,
              c.name AS category_name,
              sc.name,
              sc.status,
              sc.featured,
              sc.priority
            FROM sub_categories sc
            JOIN categories c ON c.category_id = sc.category_id
            WHERE sc.sub_category_id = :id
        """), {"id": sub_category_id}).mappings().first()

    return jsonify(dict(row)), 200


@app.route("/admin/subcategories/<int:sub_category_id>", methods=["DELETE"])
def admin_delete_subcategory(sub_category_id):
    with engine.begin() as conn:
        res = conn.execute(
            text("DELETE FROM sub_categories WHERE sub_category_id = :id"),
            {"id": sub_category_id}
        )
        if res.rowcount == 0:
            return jsonify({"error": "Sub Category not found"}), 404
    return jsonify({"ok": True}), 200


# ======================
# META (Dropdown for apps)
# ======================
@app.route("/meta/subcategories", methods=["GET"])
def meta_subcategories():
    category_id = request.args.get("category_id", type=int)
    if not category_id:
        return jsonify([]), 200

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sub_category_id, category_id, name
            FROM sub_categories
            WHERE category_id = :cid
              AND status IN ('active','ACTIVE')
            ORDER BY name ASC
        """), {"cid": category_id}).mappings().all()

    return jsonify([
        {
            "sub_category_id": int(r["sub_category_id"]),
            "category_id": int(r["category_id"]),
            "name": r["name"]
        }
        for r in rows
    ]), 200

@app.route("/admin/campaigns", methods=["GET"])
def admin_list_campaigns():
    ctype = (request.args.get("type") or "").strip().lower()
    q = (request.args.get("q") or "").strip()

    where = []
    params = {}

    if ctype:
        where.append("c.campaign_type = :ctype")
        params["ctype"] = ctype

    if q:
        where.append("(c.title LIKE :q OR c.description LIKE :q)")
        params["q"] = f"%{q}%"

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              c.*,
              z.name AS zone_name,
              s.store_name AS store_name,
              mi.name AS item_name
            FROM campaigns c
            LEFT JOIN zones z ON z.zone_id = c.zone_id
            LEFT JOIN stores s ON s.store_id = c.store_id
            LEFT JOIN menu_items mi ON mi.menu_item_id = c.menu_item_id
            {where_sql}
            ORDER BY c.campaign_id DESC
        """), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)

        # ✅ JSON-safe conversions for dates/times
        for k in ("start_date", "end_date", "created_at", "updated_at"):
            if d.get(k) is not None:
                try:
                    d[k] = d[k].isoformat()
                except Exception:
                    d[k] = str(d[k])

        for k in ("start_time", "end_time"):
            v = d.get(k)
            if v is None:
                d[k] = ""
            elif isinstance(v, timedelta):
                total = int(v.total_seconds())
                hh = (total // 3600) % 24
                mm = (total % 3600) // 60
                ss = total % 60
                d[k] = f"{hh:02d}:{mm:02d}:{ss:02d}"
            else:
                try:
                    d[k] = v.strftime("%H:%M:%S")  # datetime.time
                except Exception:
                    d[k] = str(v)

        # optional: make frontend easier (since your react uses item_id)
        d["item_id"] = d.get("menu_item_id")

        # optional: Table component sometimes needs "id"
        d["id"] = d.get("campaign_id")

        # image url
        if d.get("image_path"):
            d["imageUrl"] = resolve_image_url(d.get("image_path")) or ""
        else:
            d["imageUrl"] = ""

        out.append(d)

    return jsonify(out), 200

@app.route("/admin/campaigns", methods=["POST"])
def admin_create_campaign():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    campaign_type = (data.get("campaign_type") or "").strip().lower()
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()

    start_date = data.get("start_date")
    end_date = data.get("end_date")
    start_time = data.get("start_time")
    end_time = data.get("end_time")

    if campaign_type not in ("food", "item", "basic"):
        return jsonify({"error": "campaign_type must be food/item/basic"}), 400
    if not title or not start_date or not end_date:
        return jsonify({"error": "title, start_date, end_date required"}), 400

    image_path = None
    if is_multipart:
        f = request.files.get("image")
        if f and f.filename:
            try:
                image_path = _save_campaign_image(f)
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

    # Parse optional numeric fields
    zone_id = safe_int_or_none(data.get("zone_id"))
    store_id = safe_int_or_none(data.get("store_id"))
    menu_item_id = safe_int_or_none(data.get("menu_item_id") or data.get("item_id"))
    total_stock = safe_int_or_none(data.get("total_stock"))
    max_cart_qty = safe_int_or_none(data.get("max_cart_qty"))
    category_id = safe_int_or_none(data.get("category_id"))
    sub_category_id = safe_int_or_none(data.get("sub_category_id"))
    discount = safe_float(data.get("discount"), None) if empty_to_none(data.get("discount")) is not None else None
    discount_type = empty_to_none(data.get("discount_type"))

    # Variations JSON (only for food campaign; but allowed anyway)
    variations_json = None
    if is_multipart:
        variations_json = request.form.get("variations_json")
    else:
        variations_json = (data.get("variations_json") if isinstance(data, dict) else None)

    try:
        variations = json.loads(variations_json) if variations_json else []
    except Exception:
        return jsonify({"error": "variations_json must be valid JSON"}), 400

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO campaigns
            (campaign_type, title, description, image_path,
             start_date, end_date, start_time, end_time, status,
             zone_id, store_id, menu_item_id, total_stock, max_cart_qty,
             category_id, sub_category_id,
             discount, discount_type)
            VALUES
            (:ctype, :title, :desc, :img,
             :sd, :ed, :st, :et, 'ACTIVE',
             :zid, :sid, :mid, :stock, :mcq,
             :cat, :subcat,
             :disc, :dtype)
        """), {
            "ctype": campaign_type,
            "title": title,
            "desc": description,
            "img": image_path,
            "sd": start_date,
            "ed": end_date,
            "st": empty_to_none(start_time),
            "et": empty_to_none(end_time),
            "zid": zone_id,
            "sid": store_id,
            "mid": menu_item_id,
            "stock": total_stock,
            "mcq": max_cart_qty,
            "cat": category_id,
            "subcat": sub_category_id,
            "disc": discount,
            "dtype": discount_type
        })

        campaign_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        # Insert variations/options
        for v in variations or []:
            vname = (v.get("name") or "").strip()
            stype = (v.get("select_type") or "single").strip().lower()
            min_qty = safe_int_or_none(v.get("min"))
            max_qty = safe_int_or_none(v.get("max"))
            options = v.get("options") or []

            if not vname:
                continue
            if stype not in ("single", "multiple"):
                stype = "single"

            conn.execute(text("""
                INSERT INTO campaign_variations (campaign_id, name, select_type, min_qty, max_qty)
                VALUES (:cid, :name, :stype, :minq, :maxq)
            """), {"cid": campaign_id, "name": vname, "stype": stype, "minq": min_qty, "maxq": max_qty})

            variation_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

            for op in options:
                oname = (op.get("name") or "").strip()
                price = safe_float(op.get("price"), 0.0)
                if not oname:
                    continue
                conn.execute(text("""
                    INSERT INTO campaign_variation_options (variation_id, option_name, additional_price)
                    VALUES (:vid, :nm, :pr)
                """), {"vid": variation_id, "nm": oname, "pr": price})

    return jsonify({"ok": True, "campaign_id": campaign_id}), 201

@app.route("/admin/campaigns/<int:campaign_id>", methods=["PUT"])
def admin_update_campaign(campaign_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    # fields (same as POST)
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    start_time = data.get("start_time")
    end_time = data.get("end_time")

    zone_id = safe_int_or_none(data.get("zone_id"))
    store_id = safe_int_or_none(data.get("store_id"))
    menu_item_id = safe_int_or_none(data.get("menu_item_id") or data.get("item_id"))
    total_stock = safe_int_or_none(data.get("total_stock"))
    max_cart_qty = safe_int_or_none(data.get("max_cart_qty"))
    category_id = safe_int_or_none(data.get("category_id"))
    sub_category_id = safe_int_or_none(data.get("sub_category_id"))
    discount = safe_float(data.get("discount"), None) if empty_to_none(data.get("discount")) is not None else None
    discount_type = empty_to_none(data.get("discount_type"))

    # image (optional)
    image_path = None
    if is_multipart:
        f = request.files.get("image")
        if f and f.filename:
            image_path = _save_campaign_image(f)

    # variations json (optional)
    variations_json = request.form.get("variations_json") if is_multipart else data.get("variations_json")
    try:
        variations = json.loads(variations_json) if variations_json else []
    except Exception:
        return jsonify({"error": "variations_json must be valid JSON"}), 400

    with engine.begin() as conn:
        # update campaigns
        conn.execute(text("""
            UPDATE campaigns SET
              title=:title,
              description=:desc,
              start_date=:sd,
              end_date=:ed,
              start_time=:st,
              end_time=:et,
              zone_id=:zid,
              store_id=:sid,
              menu_item_id=:mid,
              total_stock=:stock,
              max_cart_qty=:mcq,
              category_id=:cat,
              sub_category_id=:subcat,
              discount=:disc,
              discount_type=:dtype
              {img_sql}
            WHERE campaign_id=:id
        """.format(img_sql=(", image_path=:img" if image_path else ""))), {
            "id": campaign_id,
            "title": title,
            "desc": description,
            "sd": start_date,
            "ed": end_date,
            "st": empty_to_none(start_time),
            "et": empty_to_none(end_time),
            "zid": zone_id,
            "sid": store_id,
            "mid": menu_item_id,
            "stock": total_stock,
            "mcq": max_cart_qty,
            "cat": category_id,
            "subcat": sub_category_id,
            "disc": discount,
            "dtype": discount_type,
            "img": image_path
        })

        # replace variations/options (simple approach)
        conn.execute(text("""
            DELETE o FROM campaign_variation_options o
            JOIN campaign_variations v ON v.variation_id = o.variation_id
            WHERE v.campaign_id = :cid
        """), {"cid": campaign_id})

        conn.execute(text("DELETE FROM campaign_variations WHERE campaign_id=:cid"), {"cid": campaign_id})

        for v in variations or []:
            vname = (v.get("name") or "").strip()
            stype = (v.get("select_type") or "single").strip().lower()
            min_qty = safe_int_or_none(v.get("min"))
            max_qty = safe_int_or_none(v.get("max"))
            options = v.get("options") or []
            if not vname:
                continue
            if stype not in ("single", "multiple"):
                stype = "single"

            conn.execute(text("""
                INSERT INTO campaign_variations (campaign_id, name, select_type, min_qty, max_qty)
                VALUES (:cid, :name, :stype, :minq, :maxq)
            """), {"cid": campaign_id, "name": vname, "stype": stype, "minq": min_qty, "maxq": max_qty})

            variation_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

            for op in options:
                oname = (op.get("name") or "").strip()
                price = safe_float(op.get("price"), 0.0)
                if not oname:
                    continue
                conn.execute(text("""
                    INSERT INTO campaign_variation_options (variation_id, option_name, additional_price)
                    VALUES (:vid, :nm, :pr)
                """), {"vid": variation_id, "nm": oname, "pr": price})

    return jsonify({"ok": True}), 200

@app.route("/admin/campaigns/<int:campaign_id>", methods=["DELETE"])
def admin_delete_campaign(campaign_id):
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT image_path FROM campaigns WHERE campaign_id=:id
        """), {"id": campaign_id}).mappings().first()

        if not row:
            return jsonify({"error": "Campaign not found"}), 404

        # delete DB row
        conn.execute(text("""
            DELETE FROM campaigns WHERE campaign_id=:id
        """), {"id": campaign_id})

    # ✅ delete image from GCS
    try:
        img_path = (row.get("image_path") or "").strip().lstrip("/")
        if img_path and gcs_bucket is not None:
            gcs_bucket.blob(img_path).delete()
    except Exception as e:
        print("⚠️ GCS delete failed:", e)

    return jsonify({"ok": True}), 200

# ======================
# ✅ CUSTOMER: CAMPAIGNS AS OFFERS
# ======================
@app.route("/customer/campaigns", methods=["GET"])
def customer_list_campaigns():
    zone_id = request.args.get("zoneId", type=int)
    limit = request.args.get("limit", default=50, type=int)

    where = [
        "c.status = 'ACTIVE'",
        "(c.start_date IS NULL OR c.start_date <= CURDATE())",
        "(c.end_date IS NULL OR c.end_date >= CURDATE())",
    ]
    params = {"limit": limit}

    if zone_id:
        where.append("c.zone_id = :zid")
        params["zid"] = zone_id

    where_sql = "WHERE " + " AND ".join(where)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              c.campaign_id,
              c.campaign_type,
              c.title,
              c.description,
              c.image_path,
              c.zone_id,
              c.store_id,
              c.menu_item_id,
              s.store_name,
              s.location,
              mi.name AS item_name,

              -- ✅ needed for countdown
              c.start_date,
              c.end_date,
              c.start_time,
              c.end_time

            FROM campaigns c
            LEFT JOIN stores s ON s.store_id = c.store_id
            LEFT JOIN menu_items mi ON mi.menu_item_id = c.menu_item_id
            {where_sql}
            ORDER BY c.campaign_id DESC
            LIMIT :limit
        """), params).mappings().all()

    out = []
    for r in rows:
        img = resolve_image_url(r.get("image_path")) if r.get("image_path") else ""

        # ✅ Build end datetime for countdown (Flutter expects ISO string)
        end_dt = None

        end_date = r.get("end_date")  # date
        end_time = _td_to_time(r.get("end_time"))  # time (normalized)

        if end_date and end_time:
            end_dt = dt.datetime.combine(end_date, end_time)
        elif end_date:
            # if time missing, assume end of day
            end_dt = dt.datetime.combine(end_date, dt.time(23, 59, 59))

        # ✅ Overnight fix: start 22:00 end 02:00 => end is next day
        if end_dt is not None:
            start_date = r.get("start_date")
            start_time = _td_to_time(r.get("start_time"))

            if start_date and start_time:
                start_dt = dt.datetime.combine(start_date, start_time)
                if end_dt < start_dt:
                    end_dt = end_dt + dt.timedelta(days=1)

        out.append({
            "campaignId": int(r["campaign_id"]),
            "campaignType": r.get("campaign_type") or "",
            "title": r.get("title") or "",
            "description": r.get("description") or "",
            "imageUrl": img,

            "zoneId": int(r.get("zone_id") or 0),
            "storeId": int(r.get("store_id") or 0),
            "storeName": r.get("store_name") or "",
            "area": r.get("location") or "",

            "menuItemId": int(r["menu_item_id"]) if r.get("menu_item_id") else None,
            "itemName": r.get("item_name") or "",

            "discountText": (r.get("title") or "OFF"),

            # ✅ Flutter countdown uses this
            "endTime": end_dt.isoformat() if end_dt else None,
        })

    return jsonify(out), 200

@app.route("/customer/flash-feed", methods=["GET"])
def customer_flash_feed():
    limit = request.args.get("limit", default=1, type=int)  # show 1 best popup

    with engine.connect() as conn:

        # ✅ LIVE FLASH SALES
        flash_rows = conn.execute(text("""
            SELECT flash_sale_id, title
            FROM flash_sales
            WHERE publish='Yes'
              AND start_date <= NOW()
              AND end_date >= NOW()
            ORDER BY flash_sale_id DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()

        # ✅ LIVE CAMPAIGNS (take latest)
        camp_rows = conn.execute(text("""
            SELECT campaign_id, title, image_path
            FROM campaigns
            WHERE status='ACTIVE'
              AND (start_date IS NULL OR start_date <= CURDATE())
              AND (end_date IS NULL OR end_date >= CURDATE())
            ORDER BY campaign_id DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()

    result = []

    # preference: Campaign popup first (because you have image)
    for r in camp_rows:
        result.append({
            "id": f"campaign_{r['campaign_id']}",
            "title": r.get("title") or "Offer",
            "type": "campaign",
            "imageUrl": resolve_image_url(r.get("image_path")) if r.get("image_path") else ""
        })

    # if no campaign then flash sale
    if not result:
        for r in flash_rows:
            result.append({
                "id": f"flash_{r['flash_sale_id']}",
                "title": r.get("title") or "Flash Sale",
                "type": "flash_sale",
                "imageUrl": ""  # no image in flash_sales table
            })

    return jsonify(result[:limit]), 200
         
# ======================
# UNITS (Restaurant DB)
# ======================

@app.route("/admin/units", methods=["GET"])
def admin_units_list():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT unit_id, unit
            FROM units
            WHERE status = 'active'
            ORDER BY unit ASC
        """)).mappings().all()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/admin/units", methods=["POST"])
def admin_units_create():
    data = request.json or {}
    unit = (data.get("unit") or "").strip()
    if not unit:
        return jsonify({"error": "unit is required"}), 400

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO units (unit, status)
                VALUES (:unit, 'active')
            """), {"unit": unit})

            new_id = int(conn.execute(
                text("SELECT LAST_INSERT_ID() AS id")
            ).mappings().first()["id"])

            row = conn.execute(text("""
                SELECT unit_id, unit
                FROM units
                WHERE unit_id = :id
            """), {"id": new_id}).mappings().first()

        return jsonify(dict(row)), 201

    except Exception as e:
        if "Duplicate" in str(e) or "duplicate" in str(e).lower():
            return jsonify({"error": "Unit already exists"}), 400
        return jsonify({"error": str(e)}), 500


@app.route("/admin/units/<int:unit_id>", methods=["PUT"])
def admin_units_update(unit_id):
    data = request.json or {}
    unit = (data.get("unit") or "").strip()
    if not unit:
        return jsonify({"error": "unit is required"}), 400

    try:
        with engine.begin() as conn:
            res = conn.execute(text("""
                UPDATE units
                SET unit = :unit
                WHERE unit_id = :id
            """), {"unit": unit, "id": unit_id})

            if res.rowcount == 0:
                return jsonify({"error": "Unit not found"}), 404

            row = conn.execute(text("""
                SELECT unit_id, unit
                FROM units
                WHERE unit_id = :id
            """), {"id": unit_id}).mappings().first()

        return jsonify(dict(row)), 200

    except Exception as e:
        if "Duplicate" in str(e) or "duplicate" in str(e).lower():
            return jsonify({"error": "Unit already exists"}), 400
        return jsonify({"error": str(e)}), 500


@app.route("/admin/units/<int:unit_id>", methods=["DELETE"])
def admin_units_delete(unit_id):
    with engine.begin() as conn:
        res = conn.execute(
            text("DELETE FROM units WHERE unit_id = :id"),
            {"id": unit_id}
        )
        if res.rowcount == 0:
            return jsonify({"error": "Unit not found"}), 404

    return jsonify({"message": "Unit deleted"}), 200
    
@app.route("/curations", methods=["GET"])
def curations_old_format():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              curation_id,
              name,
              image_url,
              status,
              created_at
            FROM curations
            ORDER BY curation_id ASC
        """)).mappings().all()

    out = []
    for r in rows:
        # convert NEW status -> OLD status (active/inactive)
        st = str(r.get("status") or "").strip().upper()
        old_status = "active" if st == "ACTIVE" else "inactive"

        out.append({
            "curation_id": int(r["curation_id"]),
            "category": (r.get("name") or ""),                 # ✅ OLD key
            "image_url": resolve_image_url(r.get("image_url")),# ✅ signed url
            "status": old_status,                              # ✅ lowercase
            "created_at": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else ""
        })

    return jsonify(out), 200

# ======================
# BANNERS (Admin)
# ======================

@app.route("/admin/banners", methods=["GET"])
def admin_list_banners():
    """
    Optional filters:
      ?zone=Bellampalli
      ?store_id=1
      ?status=ACTIVE
    """
    zone = (request.args.get("zone") or "").strip()
    store_id = safe_int(request.args.get("store_id", 0), 0)
    status = (request.args.get("status") or "").strip().upper()

    where = []
    params = {}

    if zone:
        where.append("b.zone = :zone")
        params["zone"] = zone

    if store_id:
        where.append("b.store_id = :sid")
        params["sid"] = store_id

    if status in ("ACTIVE", "INACTIVE"):
        where.append("b.status = :st")
        params["st"] = status

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              b.banner_id,
              b.title,
              b.zone,
              b.banner_type,
              b.store_id,
              s.store_name,
              b.is_featured,
              b.status,
              b.image_url,
              b.sort_order,
              b.created_at
            FROM banners b
            LEFT JOIN stores s ON s.store_id = b.store_id
            {where_sql}
            ORDER BY b.sort_order ASC, b.banner_id DESC
        """), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["image_url"] = resolve_image_url(d.get("image_url"))
        d["is_featured"] = int(d.get("is_featured") or 0)
        out.append(d)

    return jsonify(out), 200


@app.route("/admin/banners", methods=["POST"])
def admin_create_banner():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    title = str(data.get("title") or "").strip()
    zone = str(data.get("zone") or "").strip()
    banner_type = str(data.get("banner_type") or data.get("bannerType") or "").strip()

    if not title or not zone or not banner_type:
        return jsonify({"error": "title, zone, banner_type are required"}), 400

    store_id = safe_int_or_none(data.get("store_id"))
    is_featured = 1 if str(data.get("is_featured") or data.get("featured") or "").strip().lower() in ("1","true","yes") else 0

    st_raw = str(data.get("status") or "ACTIVE").strip().upper()
    status = "ACTIVE" if st_raw in ("ACTIVE","1","TRUE","YES") else "INACTIVE"

    sort_order = safe_int(data.get("sort_order", 0), 0)

    image_url_value = empty_to_none(data.get("image_url"))

    # image upload
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                image_url_value = upload_file_to_gcs(f, folder="banners")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO banners
              (title, zone, banner_type, store_id, is_featured, status, image_url, sort_order)
            VALUES
              (:title, :zone, :btype, :sid, :feat, :status, :img, :sort)
        """), {
            "title": title,
            "zone": zone,
            "btype": banner_type,
            "sid": store_id,
            "feat": is_featured,
            "status": status,
            "img": image_url_value,
            "sort": sort_order
        })

        banner_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        row = conn.execute(text("""
            SELECT b.*, s.store_name
            FROM banners b
            LEFT JOIN stores s ON s.store_id = b.store_id
            WHERE b.banner_id = :bid
        """), {"bid": banner_id}).mappings().first()

    d = dict(row)
    d["image_url"] = resolve_image_url(d.get("image_url"))
    d["is_featured"] = int(d.get("is_featured") or 0)
    return jsonify(d), 201


@app.route("/admin/banners/<int:banner_id>", methods=["PUT"])
def admin_update_banner(banner_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    updates = {}

    if "title" in data: updates["title"] = empty_to_none(str(data.get("title") or "").strip())
    if "zone" in data: updates["zone"] = empty_to_none(str(data.get("zone") or "").strip())
    if "banner_type" in data or "bannerType" in data:
        updates["banner_type"] = empty_to_none(str(data.get("banner_type") or data.get("bannerType") or "").strip())

    if "store_id" in data:
        updates["store_id"] = safe_int_or_none(data.get("store_id"))

    if "is_featured" in data or "featured" in data:
        v = str(data.get("is_featured") or data.get("featured") or "").strip().lower()
        updates["is_featured"] = 1 if v in ("1","true","yes") else 0

    if "status" in data:
        v = str(data.get("status") or "").strip().upper()
        updates["status"] = "ACTIVE" if v in ("ACTIVE","1","TRUE","YES") else "INACTIVE"

    if "sort_order" in data:
        updates["sort_order"] = safe_int(data.get("sort_order", 0), 0)

    # image upload
    try:
        if is_multipart and "image" in request.files:
            f = request.files["image"]
            if f and f.filename:
                updates["image_url"] = upload_file_to_gcs(f, folder=f"banners/{banner_id}")
        else:
            if "image_url" in data:
                updates["image_url"] = empty_to_none(str(data.get("image_url") or "").strip())
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["bid"] = banner_id
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "bid"])

    with engine.begin() as conn:
        res = conn.execute(text(f"""
            UPDATE banners SET {set_clause}
            WHERE banner_id = :bid
        """), updates)

        if res.rowcount == 0:
            return jsonify({"error": "Banner not found"}), 404

        row = conn.execute(text("""
            SELECT b.*, s.store_name
            FROM banners b
            LEFT JOIN stores s ON s.store_id = b.store_id
            WHERE b.banner_id = :bid
        """), {"bid": banner_id}).mappings().first()

    d = dict(row)
    d["image_url"] = resolve_image_url(d.get("image_url"))
    d["is_featured"] = int(d.get("is_featured") or 0)
    return jsonify(d), 200


@app.route("/admin/banners/<int:banner_id>", methods=["DELETE"])
def admin_delete_banner(banner_id):
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM banners WHERE banner_id = :bid"), {"bid": banner_id})
        if res.rowcount == 0:
            return jsonify({"error": "Banner not found"}), 404
    return jsonify({"message": "Banner deleted"}), 200

# ======================
# BANNERS (Customer App)
# ======================
@app.route("/banners", methods=["GET"])
def public_list_banners():
    """
    Customer app banners:
      /banners?zone=Bellampalli
      - returns ONLY ACTIVE by default
      - sorted by sort_order
    """
    zone = (request.args.get("zone") or "").strip()

    where = ["b.status = 'ACTIVE'"]   # ✅ only active banners
    params = {}

    if zone:
        where.append("b.zone = :zone")
        params["zone"] = zone

    where_sql = "WHERE " + " AND ".join(where)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              b.banner_id,
              b.title,
              b.zone,
              b.banner_type,
              b.store_id,
              s.store_name,
              b.is_featured,
              b.status,
              b.image_url,
              b.sort_order
            FROM banners b
            LEFT JOIN stores s ON s.store_id = b.store_id
            {where_sql}
            ORDER BY b.sort_order ASC, b.banner_id DESC
        """), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["image_url"] = resolve_image_url(d.get("image_url"))
        d["is_featured"] = int(d.get("is_featured") or 0)
        out.append(d)

    return jsonify(out), 200

@app.route("/top-picks-stores", methods=["GET"])
def top_picks_stores():
    limit = safe_int(request.args.get("limit", 5), 5)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS address,
              COALESCE(s.city, '') AS location,
              s.logo_url,
              s.status,
              COUNT(o.order_id) AS order_count,
              (
                SELECT GROUP_CONCAT(mi.name ORDER BY mi.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi
                WHERE mi.store_id = s.store_id
                  AND mi.status = 'ACTIVE'
                LIMIT 3
              ) AS items_preview
            FROM stores s
            LEFT JOIN orders o
              ON o.store_id = s.store_id
            WHERE s.status = 'ACTIVE'
            GROUP BY s.store_id, s.store_name, s.logo_url, address, location, s.status
            ORDER BY order_count DESC, s.store_id DESC
            LIMIT :lim
        """), {"lim": limit}).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["status"] = True if str(d.get("status")) == "ACTIVE" else False
        d["items_preview"] = d.get("items_preview") or ""
        out.append(d)

    return jsonify(out), 200

@app.route("/curations/<int:curation_id>/stores", methods=["GET"])
def get_stores_by_curation(curation_id):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              s.address_line1,
              s.address_line2,
              s.city,
              s.state,
              s.pincode,
              s.logo_url,
              s.status,
              cs.sort_order,

              -- ✅ items preview (latest 6 items)
              (
                SELECT GROUP_CONCAT(mi.name ORDER BY mi.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi
                WHERE mi.store_id = s.store_id
                LIMIT 6
              ) AS items_preview

            FROM curation_stores cs
            JOIN stores s ON s.store_id = cs.store_id
            WHERE cs.curation_id = :cid
              AND cs.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
            ORDER BY cs.sort_order ASC, cs.id ASC
        """), {"cid": curation_id}).mappings().all()

    out = []
    for r in rows:
        address = " ".join([x for x in [
            r.get("address_line1"),
            r.get("address_line2"),
            r.get("city"),
            r.get("state"),
            r.get("pincode"),
        ] if x])

        out.append({
            "store_id": int(r.get("store_id") or 0),
            "store_name": r.get("store_name") or "",
            "address": address,
            "zone": "",                                # your app has it; DB may not
            "location": r.get("city") or "",            # you can change to zone later
            "logo_url": resolve_image_url(r.get("logo_url")),
            "status": True,
            "items_preview": r.get("items_preview") or "",
        })

    return jsonify(out), 200

def spotlight_time_sections():
    """
    Restaurant-only timing:
      06-11 -> Breakfast -> Tiffins
      11-16 -> Lunch     -> Food
      16-19 -> Snacks    -> Snacks, Drinks
      else  -> Dinner    -> Food
    """
    hr = datetime.now().hour
    if 6 <= hr < 11:
        return "Breakfast", ["Tiffins"]
    elif 11 <= hr < 16:
        return "Lunch", ["Food"]
    elif 16 <= hr < 19:
        return "Snacks", ["Snacks", "Drinks"]
    else:
        return "Dinner", ["Food"]
    
# ======================
# SPOTLIGHT (TIME BASED)
# Flutter expects:
# { "label": "...", "data": [ {storeId, storeName, itemName, price, imageUrl, address} ] }
# ======================
@app.route("/spotlight", methods=["GET"])
def spotlight():
    # optional params
    limit = safe_int(request.args.get("limit", 10), 10)
    store_id = safe_int(request.args.get("store_id", 0), 0)

    # manual override if you pass ?section=Food or ?category=Food
    manual_section = (request.args.get("section") or request.args.get("category") or "").strip()

    # ✅ if not manual, use timing logic
    if manual_section:
        label = manual_section
        sections = [manual_section]
    else:
        label, sections = spotlight_time_sections()

    where = ["mi.status = 'ACTIVE'"]
    params = {"lim": limit}

    if store_id:
        where.append("mi.store_id = :sid")
        params["sid"] = store_id

    # section filter (menu_sections.name)
    if sections:
        # expand IN list safely
        where.append("ms.name IN :secs")
        params["secs"] = tuple(sections)

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            mi.menu_item_id,
            mi.name AS item_name,
            mi.image_url,
            mi.store_id,
            s.store_name,
            s.address_line1, s.address_line2, s.city, s.state, s.pincode,

            v.variant_id,
            v.variant_name,
            v.price,

            ms.name AS section_name
        FROM menu_items mi
        LEFT JOIN stores s ON s.store_id = mi.store_id
        LEFT JOIN menu_sections ms ON ms.section_id = mi.section_id
        LEFT JOIN (
            SELECT menu_item_id, MIN(variant_id) AS first_variant_id
            FROM menu_item_variants
            WHERE status = 'ACTIVE'
            GROUP BY menu_item_id
        ) fv ON fv.menu_item_id = mi.menu_item_id
        LEFT JOIN menu_item_variants v ON v.variant_id = fv.first_variant_id
        {where_sql}
        ORDER BY mi.menu_item_id DESC
        LIMIT :lim
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    data = []
    for r in rows:
        addr = " ".join([x for x in [
            r.get("address_line1"),
            r.get("address_line2"),
            r.get("city"),
            r.get("state"),
            r.get("pincode")
        ] if x])

        data.append({
            "menu_item_id": int(r.get("menu_item_id") or 0),
            "store_id": int(r.get("store_id") or 0),
            "store_name": r.get("store_name") or "",
            "address": addr,
            "item_name": r.get("item_name") or "",
            "price": float(r.get("price") or 0.0),
            "image_url": resolve_image_url(r.get("image_url")),
            # optional extras (Flutter ignores if not used)
            "variant_id": int(r.get("variant_id") or 0),
            "variant_name": r.get("variant_name") or "Regular",
        })

    return jsonify({"label": label, "data": data}), 200

# ======================
# POPULAR ITEMS (Top 10 by store count)
# ======================
@app.route("/popular-items", methods=["GET"])
def popular_items():
    limit = safe_int(request.args.get("limit", 10), 10)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              mi.name AS item_name,
              MAX(mi.image_url) AS image_url,
              COUNT(DISTINCT mi.store_id) AS store_count
            FROM menu_items mi
            WHERE mi.status = 'ACTIVE'
              AND mi.name IS NOT NULL
              AND mi.name <> ''
            GROUP BY mi.name
            ORDER BY store_count DESC, COUNT(*) DESC, mi.name ASC
            LIMIT :lim
        """), {"lim": limit}).mappings().all()

    out = []
    for r in rows:
        out.append({
            "item_name": r.get("item_name") or "",
            "image_url": resolve_image_url(r.get("image_url")),
            "store_count": int(r.get("store_count") or 0),
        })

    return jsonify(out), 200


# ======================
# STORES BY ITEM NAME
# ======================
@app.route("/popular-items/stores", methods=["GET"])
def popular_item_stores():
    item_name = (request.args.get("item_name") or "").strip()
    if not item_name:
        return jsonify({"error": "item_name is required"}), 400

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS address,
              COALESCE(s.city, '') AS location,
              s.logo_url,
              s.status,
              (
                SELECT GROUP_CONCAT(mi2.name ORDER BY mi2.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi2
                WHERE mi2.store_id = s.store_id
                  AND mi2.status = 'ACTIVE'
                LIMIT 3
              ) AS items_preview
            FROM stores s
            WHERE s.status = 'ACTIVE'
              AND EXISTS (
                SELECT 1
                FROM menu_items mi
                WHERE mi.store_id = s.store_id
                  AND mi.status = 'ACTIVE'
                  AND mi.name = :iname
              )
            ORDER BY s.store_id DESC
        """), {"iname": item_name}).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["status"] = True
        d["items_preview"] = d.get("items_preview") or ""
        out.append(d)

    return jsonify(out), 200

# ======================
# TOP PICKS FOR YOU (based on customer order history)
# GET /top-picks?phone=XXXXXXXXXX&limit=5
# ======================
@app.route("/top-picks", methods=["GET"])
def top_picks():
    phone = (request.args.get("phone") or "").strip()
    limit = safe_int(request.args.get("limit", 5), 5)

    if not phone:
        return jsonify({"error": "phone is required"}), 400

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS address,
              COALESCE(s.city, '') AS location,
              s.logo_url,
              COUNT(*) AS order_count,
              (
                SELECT GROUP_CONCAT(mi.name ORDER BY mi.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi
                WHERE mi.store_id = s.store_id
                  AND mi.status = 'ACTIVE'
                LIMIT 3
              ) AS items_preview
            FROM orders o
            JOIN customers c ON c.customer_id = o.customer_id
            JOIN stores s ON s.store_id = o.store_id
            WHERE c.phone = :ph
            GROUP BY s.store_id, s.store_name, s.logo_url, address, location
            ORDER BY order_count DESC, MAX(o.created_at) DESC
            LIMIT :lim
        """), {"ph": phone, "lim": limit}).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["status"] = True
        d["items_preview"] = d.get("items_preview") or ""
        out.append(d)

    return jsonify(out), 200

@app.route("/search", methods=["GET"])
def search_all():
    """
    Search stores + items in one API
    GET /search?q=pizza&limit=10

    Returns:
    {
      "stores": [ {store_id, store_name, address, location, logo_url, items_preview} ],
      "items":  [ {item_name, image_url, store_count} ]
    }
    """
    q = (request.args.get("q") or "").strip()
    limit = safe_int(request.args.get("limit", 10), 10)

    if not q:
        return jsonify({"stores": [], "items": []}), 200

    like = f"%{q}%"

    with engine.connect() as conn:
        # ✅ STORES SEARCH: name, city, items_preview
        stores = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS address,
              COALESCE(s.city,'') AS location,
              s.logo_url,
              (
                SELECT GROUP_CONCAT(mi.name ORDER BY mi.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi
                WHERE mi.store_id = s.store_id AND mi.status='ACTIVE'
                LIMIT 3
              ) AS items_preview
            FROM stores s
            WHERE s.status='ACTIVE'
              AND (
                s.store_name LIKE :like
                OR s.city LIKE :like
                OR EXISTS (
                    SELECT 1 FROM menu_items mi
                    WHERE mi.store_id = s.store_id
                      AND mi.status='ACTIVE'
                      AND mi.name LIKE :like
                )
              )
            ORDER BY s.store_id DESC
            LIMIT :lim
        """), {"like": like, "lim": limit}).mappings().all()

        # ✅ ITEMS SEARCH: item name (popular-items style)
        items = conn.execute(text("""
            SELECT
              mi.name AS item_name,
              MAX(mi.image_url) AS image_url,
              COUNT(DISTINCT mi.store_id) AS store_count
            FROM menu_items mi
            WHERE mi.status='ACTIVE'
              AND mi.name IS NOT NULL
              AND mi.name <> ''
              AND mi.name LIKE :like
            GROUP BY mi.name
            ORDER BY store_count DESC, mi.name ASC
            LIMIT :lim
        """), {"like": like, "lim": limit}).mappings().all()

    out_stores = []
    for r in stores:
        out_stores.append({
            "store_id": int(r.get("store_id") or 0),
            "store_name": r.get("store_name") or "",
            "address": r.get("address") or "",
            "location": r.get("location") or "",
            "logo_url": resolve_image_url(r.get("logo_url")),
            "items_preview": r.get("items_preview") or "",
        })

    out_items = []
    for r in items:
        out_items.append({
            "item_name": r.get("item_name") or "",
            "image_url": resolve_image_url(r.get("image_url")),
            "store_count": int(r.get("store_count") or 0),
        })

    return jsonify({"stores": out_stores, "items": out_items}), 200

# ======================
# CUSTOMER → PLACE CART ORDER (updated to new schema)
# keeps old route name /place-cart-order
# ======================
@app.route("/place-cart-order", methods=["POST"])
def place_cart_order():
    data = request.json or {}

    customer_name = str(data.get("customer_name", "")).strip()
    address_text = str(data.get("address", "")).strip()
    phone_number = str(data.get("phone_number", "")).strip()
    store_id = safe_int(data.get("store_id", 0), 0)

    if not phone_number:
        return jsonify({"error": "phone_number is required"}), 400
    if store_id == 0:
        return jsonify({"error": "store_id is required"}), 400

    items = data.get("items", []) or []
    if not items:
        return jsonify({"error": "items required"}), 400

    with engine.begin() as conn:
        # ensure customer
        cust = conn.execute(text("""
            SELECT customer_id FROM customers WHERE phone = :ph LIMIT 1
        """), {"ph": phone_number}).mappings().first()

        if cust:
            customer_id = int(cust["customer_id"])
        else:
            conn.execute(text("""
                INSERT INTO customers (name, phone, status)
                VALUES (:name, :phone, 'ACTIVE')
            """), {"name": customer_name or None, "phone": phone_number})
            customer_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        # create address row (simple)
        address_id = None
        if address_text:
            conn.execute(text("""
                INSERT INTO customer_addresses
                (customer_id, label, address_line1, address_line2, city, state, pincode, is_default)
                VALUES (:cid, 'Home', :a1, NULL, NULL, NULL, NULL, 1)
            """), {"cid": customer_id, "a1": address_text})
            address_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

        # compute totals from items
        subtotal = 0.0
        for it in items:
            qty = safe_int(it.get("quantity", 1), 1)
            price = safe_float(it.get("price", 0), 0.0)
            subtotal += (price * qty)

        # get packing charge from store
        st = conn.execute(text("""
            SELECT packing_charge FROM stores WHERE store_id = :sid
        """), {"sid": store_id}).mappings().first()
        packing_charge = float(st.get("packing_charge") or 0.0) if st else 0.0

        delivery_fee = safe_float(data.get("delivery_fee", 0), 0.0)
        discount_amount = safe_float(data.get("discount_amount", 0), 0.0)
        tax_amount = safe_float(data.get("tax_amount", 0), 0.0)

        grand_total = max(0.0, subtotal + packing_charge + delivery_fee + tax_amount - discount_amount)

        # create order
        conn.execute(text("""
            INSERT INTO orders
            (order_number, customer_id, store_id, delivery_address_id,
             payment_method, payment_status, order_status,
             subtotal, packing_charge, delivery_fee, discount_amount, tax_amount, grand_total,
             notes)
            VALUES
            ('TEMP', :cid, :sid, :addr,
             :pm, 'PENDING', 'PLACED',
             :subtotal, :packing, :delivery, :discount, :tax, :grand,
             :notes)
        """), {
            "cid": customer_id,
            "sid": store_id,
            "addr": address_id,
            "pm": (data.get("payment_method") or "COD"),
            "subtotal": round(subtotal, 2),
            "packing": round(packing_charge, 2),
            "delivery": round(delivery_fee, 2),
            "discount": round(discount_amount, 2),
            "tax": round(tax_amount, 2),
            "grand": round(grand_total, 2),
            "notes": empty_to_none(data.get("notes"))
        })

        order_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])
        order_number = make_order_number(order_id)

        conn.execute(text("""
            UPDATE orders SET order_number = :onum WHERE order_id = :oid
        """), {"onum": order_number, "oid": order_id})

        # insert items
        # insert items
        for it in items:
            qty = safe_int(it.get("quantity", 1), 1)
            unit_price = safe_float(it.get("price", 0), 0.0)
            line_total = round(unit_price * qty, 2)

            item_name = str(it.get("product_name") or it.get("item_name") or "").strip()

            menu_item_id = safe_int(it.get("menu_item_id", 0), 0)
            variant_id = safe_int_or_none(it.get("variant_id"))

            # ✅ If customer app didn't send menu_item_id, lookup by (store_id + name)
            if not menu_item_id:
                row = conn.execute(text("""
                    SELECT menu_item_id
                    FROM menu_items
                    WHERE store_id = :sid AND name = :nm
                    ORDER BY menu_item_id DESC
                    LIMIT 1
                """), {"sid": store_id, "nm": item_name}).mappings().first()

                if not row:
                    return jsonify({
                        "error": "menu_item_id missing and item not found in menu_items",
                        "store_id": store_id,
                        "item_name": item_name
                    }), 400

                menu_item_id = int(row["menu_item_id"])

            conn.execute(text("""
                INSERT INTO order_items
                (order_id, menu_item_id, variant_id,
                item_name_snapshot, variant_name_snapshot,
                unit_price, qty, line_total)
                VALUES
                (:oid, :mid, :vid,
                :iname, :vname,
                :unit_price, :qty, :line_total)
            """), {
                "oid": order_id,
                "mid": menu_item_id,   # ✅ always valid now
                "vid": variant_id,
                "iname": item_name or "Item",
                "vname": empty_to_none(it.get("variant_name")),
                "unit_price": round(unit_price, 2),
                "qty": qty,
                "line_total": line_total
            })

        return jsonify({
            "message": "✅ Cart Order Placed Successfully",
            "order_id": order_id,
            "order_number": order_number
        }), 201


# ======================
# ADMIN → GET FULL ORDERS (updated)
# keeps old route name /get-full-orders
# ======================
@app.route("/get-full-orders", methods=["GET"])
def get_full_orders():
    with engine.connect() as conn:
        orders_rows = conn.execute(text("""
            SELECT
              o.order_id, o.order_number, o.store_id, o.customer_id,
              o.subtotal, o.grand_total, o.order_status, o.created_at,
              o.notes,
              c.name AS customer_name, c.phone AS phone_number,
              ca.address_line1 AS address,
              s.store_name AS store_name
            FROM orders o
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            LEFT JOIN stores s ON s.store_id = o.store_id
            ORDER BY o.order_id DESC
        """)).mappings().all()

        items_rows = conn.execute(text("""
            SELECT
              oi.order_id,
              oi.item_name_snapshot AS product_name,
              oi.unit_price AS price,
              oi.qty AS quantity
            FROM order_items oi
            ORDER BY oi.order_id DESC
        """)).mappings().all()

    items_by_order = {}
    for it in items_rows:
        items_by_order.setdefault(int(it["order_id"]), []).append({
            "product_name": it.get("product_name", ""),
            "price": float(it.get("price") or 0),
            "quantity": int(it.get("quantity") or 0),
        })

    out = []
    for o in orders_rows:
        out.append({
            "order_id": int(o["order_id"]),
            "order_number": o.get("order_number") or "",
            "store_id": int(o.get("store_id") or 0),
            "store_name": o.get("store_name") or "",      # ✅ added
            "customer_name": o.get("customer_name") or "",
            "phone_number": o.get("phone_number") or "",
            "address": o.get("address") or "",
            "total_price": float(o.get("grand_total") or 0),
            "date": o["created_at"].strftime("%Y-%m-%d %H:%M") if o.get("created_at") else "",
            "status": str(o.get("order_status") or "PLACED"),
            "notes": o.get("notes") or "",                # ✅ added (your UI uses o.notes)
            "items": items_by_order.get(int(o["order_id"]), [])
        })

    return jsonify(out), 200

@app.route("/vendor/orders", methods=["GET"])
def vendor_orders_safe():
    vendor_id = safe_int(request.args.get("vendor_id", 0), 0)
    store_id  = safe_int(request.args.get("store_id", 0), 0)
    status_q  = (request.args.get("status") or "").strip().upper()

    if not vendor_id:
        return jsonify({"error": "vendor_id is required"}), 400

    # Build WHERE safely
    where = ["s.vendor_id = :vid"]
    params = {"vid": vendor_id}

    if store_id:
        where.append("o.store_id = :sid")
        params["sid"] = store_id

    if status_q:
        where.append("UPPER(TRIM(o.order_status)) = :st")
        params["st"] = status_q

    where_sql = "WHERE " + " AND ".join(where)

    with engine.connect() as conn:
        orders_rows = conn.execute(text(f"""
            SELECT
              o.order_id, o.order_number, o.created_at, o.order_status, o.grand_total,
              c.name AS customer_name, c.phone AS phone_number,
              ca.address_line1 AS address
            FROM orders o
            JOIN stores s ON s.store_id = o.store_id
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            {where_sql}
            ORDER BY o.order_id DESC
        """), params).mappings().all()

        items_rows = conn.execute(text(f"""
            SELECT
              oi.order_id,
              oi.item_name_snapshot AS product_name,
              oi.unit_price AS price,
              oi.qty AS quantity
            FROM order_items oi
            WHERE oi.order_id IN (
              SELECT o.order_id
              FROM orders o
              JOIN stores s ON s.store_id = o.store_id
              {where_sql}
            )
            ORDER BY oi.order_id DESC
        """), params).mappings().all()

    items_by_order = {}
    for it in items_rows:
        items_by_order.setdefault(int(it["order_id"]), []).append({
            "product_name": it.get("product_name", ""),
            "price": float(it.get("price") or 0),
            "quantity": int(it.get("quantity") or 0),
        })

    out = []
    for o in orders_rows:
        out.append({
            "id": int(o["order_id"]),
            "order_number": o.get("order_number") or "",
            "date": o["created_at"].strftime("%Y-%m-%d %H:%M") if o.get("created_at") else "",
            "status": (o.get("order_status") or "").strip(),
            "type": "Delivery",
            "total": float(o.get("grand_total") or 0),
            "items": items_by_order.get(int(o["order_id"]), []),
            "customer": {
                "name": o.get("customer_name") or "",
                "address": o.get("address") or "",
                "phone": o.get("phone_number") or "",
            }
        })

    return jsonify(out), 200

@app.route("/orders/<int:order_id>", methods=["GET"])
def get_order_details(order_id):
    with engine.connect() as conn:
        o = conn.execute(text("""
            SELECT
                o.order_id,
                o.order_number,
                o.created_at,
                o.order_status,
                o.rider_id,

                o.subtotal,
                o.packing_charge,
                o.delivery_fee,
                o.discount_amount,
                o.tax_amount,
                o.grand_total,

                s.store_id,
                s.store_name,
                s.logo_url,
                CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS store_address,

                c.customer_id,
                c.name AS customer_name,
                c.phone AS customer_phone,

                ca.address_line1 AS customer_address,

                r.name  AS rider_name,
                r.phone AS rider_phone,
                r.email AS rider_email

                FROM orders o
                LEFT JOIN stores s ON s.store_id = o.store_id
                LEFT JOIN customers c ON c.customer_id = o.customer_id
                LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
                LEFT JOIN riders r ON r.rider_id = o.rider_id
                WHERE o.order_id = :oid
                LIMIT 1
        """), {"oid": order_id}).mappings().first()

        if not o:
            return jsonify({"error": "Order not found"}), 404

        # ✅ items + image_url (join menu_items)
        items = conn.execute(text("""
            SELECT
              oi.order_item_id,
              oi.menu_item_id,
              oi.item_name_snapshot AS name,
              oi.qty AS qty,
              oi.unit_price AS price,
              oi.line_total AS line_total,
              mi.image_url AS item_image_url
            FROM order_items oi
            LEFT JOIN menu_items mi ON mi.menu_item_id = oi.menu_item_id
            WHERE oi.order_id = :oid
            ORDER BY oi.order_item_id ASC
        """), {"oid": order_id}).mappings().all()

    store_addr = (o.get("store_address") or "").strip()
    cust_addr  = (o.get("customer_address") or "").strip()

    # ✅ safe google maps links
    store_map = f"https://maps.google.com/?q={quote_plus(store_addr)}" if store_addr else ""
    cust_map  = f"https://maps.google.com/?q={quote_plus(cust_addr)}" if cust_addr else ""

    # ✅ totals (fallback safe)
    subtotal        = float(o.get("subtotal") or 0)
    packing_charge  = float(o.get("packing_charge") or 0)   # you can show as "Platform fee" if you want
    delivery_fee    = float(o.get("delivery_fee") or 0)
    discount_amount = float(o.get("discount_amount") or 0)
    tax_amount      = float(o.get("tax_amount") or 0)
    tips_amount     = 0.0  # if you add tips column later, update here
    grand_total     = float(o.get("grand_total") or 0)

    return jsonify({
        "id": o.get("order_number") or str(o.get("order_id")),
        "order_id": int(o.get("order_id") or 0),
        "order_number": o.get("order_number") or "",
        "date": o["created_at"].strftime("%Y-%m-%d %H:%M") if o.get("created_at") else "",
        "total": grand_total,
        "status": str(o.get("order_status") or ""),

        # ✅ NEW: bill breakup (BillDetails can use this directly)
        "totals": {
            "item_mrp": subtotal,                 # or compute from items if needed
            "items_price": subtotal,
            "sub_total": subtotal,
            "discount": discount_amount,
            "coupon_discount": 0.0,               # if you add coupon later
            "packing_charge": packing_charge,     # you can label it "Platform fee" in UI
            "platform_fee": packing_charge,       # alias for UI convenience
            "gst": tax_amount,
            "tax": tax_amount,
            "delivery_fee": delivery_fee,
            "deliveryman_tips": tips_amount,
            "total": grand_total
        },

        "store": {
            "store_id": int(o.get("store_id") or 0),
            "name": o.get("store_name") or "",
            "address": store_addr,
            "location": store_map,

            # ✅ NEW: store logo (signed URL if stored in GCS path)
            "logo_url": resolve_image_url(o.get("logo_url")),
        },

        "customer": {
            "customer_id": int(o.get("customer_id") or 0),
            "name": o.get("customer_name") or "",
            "phone": o.get("customer_phone") or "",
            "email": "",
            "address": cust_addr,
            "location": cust_map,
        },

        "deliveryMan": {
            "name": o.get("rider_name") or "",
            "phone": o.get("rider_phone") or "",
            "email": o.get("rider_email") or "",
            "location": "",
            "orders_delivered": 0,     # optional (if you want later we can compute)
            "last_location_text": "",  # optional (if you store location later)
            "avatar_url": ""           # riders table doesn't have it now
        },

        "items": [{
            "name": it.get("name") or "",
            "qty": int(it.get("qty") or 0),
            "price": float(it.get("price") or 0),
            "line_total": float(it.get("line_total") or 0),

            # ✅ NEW: item image
            "image_url": resolve_image_url(it.get("item_image_url")),
        } for it in items]
    }), 200

# ======================
# CUSTOMER → MY ORDERS (Running + History)
# URL:
#  /customer/my-orders?customer_id=10&tab=running
#  /customer/my-orders?customer_id=10&tab=history
# ======================
@app.route("/customer/my-orders", methods=["GET"])
def customer_my_orders():
    customer_id = safe_int(request.args.get("customer_id", 0), 0)
    if not customer_id:
        return jsonify({"error": "customer_id is required"}), 400

    tab = (request.args.get("tab") or "running").strip().lower()

    running_status = ("PLACED", "ACCEPTED", "PREPARING", "READY", "PICKED_UP")
    history_status = ("DELIVERED", "CANCELLED")

    statuses = running_status if tab == "running" else history_status

    with engine.connect() as conn:
        orders_rows = conn.execute(text("""
            SELECT
              o.order_id,
              o.order_number,
              o.store_id,
              o.customer_id,
              o.order_status,
              o.grand_total,
              o.created_at,
              s.store_name AS store_name
            FROM orders o
            LEFT JOIN stores s ON s.store_id = o.store_id
            WHERE o.customer_id = :cid
              AND UPPER(TRIM(o.order_status)) IN :statuses
            ORDER BY o.order_id DESC
        """), {"cid": customer_id, "statuses": tuple(statuses)}).mappings().all()

        # Items only for these orders
        order_ids = [int(o["order_id"]) for o in orders_rows]
        items_by_order = {}

        if order_ids:
            items_rows = conn.execute(text("""
                SELECT
                  oi.order_id,
                  oi.item_name_snapshot AS product_name,
                  oi.unit_price AS price,
                  oi.qty AS quantity
                FROM order_items oi
                WHERE oi.order_id IN :ids
                ORDER BY oi.order_id DESC
            """), {"ids": tuple(order_ids)}).mappings().all()

            for it in items_rows:
                oid = int(it["order_id"])
                items_by_order.setdefault(oid, []).append({
                    "product_name": it.get("product_name") or "",
                    "price": float(it.get("price") or 0),
                    "quantity": int(it.get("quantity") or 0),
                })

    out = []
    for o in orders_rows:
        oid = int(o["order_id"])
        out.append({
            "order_id": oid,
            "order_number": o.get("order_number") or "",
            "store_id": int(o.get("store_id") or 0),
            "store_name": o.get("store_name") or "",
            "date": o["created_at"].strftime("%Y-%m-%d %H:%M") if o.get("created_at") else "",
            "status": (o.get("order_status") or "").strip(),
            "total": float(o.get("grand_total") or 0),
            "items": items_by_order.get(oid, []),
        })

    return jsonify(out), 200

@app.route("/customer/order-details", methods=["GET"])
def customer_order_details():
    order_id = request.args.get("order_id", type=int)
    customer_id = request.args.get("customer_id", type=int)

    if not order_id or not customer_id:
        return jsonify({"error": "order_id and customer_id are required"}), 400

    with engine.connect() as conn:
        # 1) order + store + customer + address + rider
        row = conn.execute(text("""
            SELECT
              o.order_id,
              o.order_number,
              o.customer_id,
              o.store_id,
              o.rider_id,
              o.delivery_address_id,
              o.payment_method,
              o.payment_status,
              o.order_status,
              o.subtotal,
              o.packing_charge,
              o.delivery_fee,
              o.discount_amount,
              o.tax_amount,
              o.grand_total,
              o.notes,
              o.created_at,

              s.store_name,
              s.zone AS store_zone,
              s.address_line1 AS store_address_line1,
              s.address_line2 AS store_address_line2,
              s.city AS store_city,
              s.state AS store_state,
              s.pincode AS store_pincode,
              s.phone AS store_phone,
              s.logo_url AS store_logo_url,

              c.name AS customer_name,
              c.phone AS customer_phone,

              ca.label AS address_label,
              ca.address_line1 AS customer_address_line1,
              ca.address_line2 AS customer_address_line2,
              ca.city AS customer_city,
              ca.state AS customer_state,
              ca.pincode AS customer_pincode,
              ca.latitude,
              ca.longitude,

              r.name AS rider_name
            FROM orders o
            LEFT JOIN stores s ON s.store_id = o.store_id
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            LEFT JOIN riders r ON r.rider_id = o.rider_id
            WHERE o.order_id = :oid AND o.customer_id = :cid
            LIMIT 1
        """), {"oid": order_id, "cid": customer_id}).mappings().first()

        if not row:
            return jsonify({"error": "Order not found"}), 404

        # 2) items (REAL order_items columns)
        items = conn.execute(text("""
            SELECT
              oi.order_item_id,
              oi.menu_item_id,
              oi.variant_id,
              oi.item_name_snapshot,
              oi.variant_name_snapshot,
              oi.unit_price,
              oi.qty,
              oi.line_total,
              oi.created_at,

              mi.is_veg
            FROM order_items oi
            LEFT JOIN menu_items mi ON mi.menu_item_id = oi.menu_item_id
            WHERE oi.order_id = :oid
            ORDER BY oi.order_item_id ASC
        """), {"oid": order_id}).mappings().all()

        # 3) addons (SAFE - handles different schemas)
        def _get_columns(conn, table_name: str):
            cols = conn.execute(text("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t
            """), {"t": table_name}).scalars().all()
            return set(cols or [])

        addons = []
        try:
            oia_cols = _get_columns(conn, "order_item_addons")
            a_cols = _get_columns(conn, "addons")

            if not oia_cols:
                addons = []  # table not found or no columns
            else:
                # choose name expression
                join_addons = ("addon_id" in oia_cols) and ("addons" in [x.lower() for x in ["addons"]])
                has_addon_name = "addon_name" in oia_cols

                name_expr = "oia.addon_name" if has_addon_name else (
                    "a.name" if ("addon_id" in oia_cols and "name" in a_cols) else "''"
                )

                # choose price expression
                if "price" in oia_cols:
                    price_expr = "oia.price"
                elif "addon_price" in oia_cols:
                    price_expr = "oia.addon_price"
                elif ("addon_id" in oia_cols and "price" in a_cols):
                    price_expr = "a.price"
                else:
                    price_expr = "0"

                join_sql = ""
                if ("addon_id" in oia_cols) and ("name" in a_cols or "price" in a_cols):
                    join_sql = "LEFT JOIN addons a ON a.addon_id = oia.addon_id"

                addons = conn.execute(text(f"""
                    SELECT
                    oia.order_item_id,
                    {name_expr} AS addon_name,
                    {price_expr} AS price
                    FROM order_item_addons oia
                    JOIN order_items oi ON oi.order_item_id = oia.order_item_id
                    {join_sql}
                    WHERE oi.order_id = :oid
                    ORDER BY oia.order_item_id ASC
                """), {"oid": order_id}).mappings().all()

        except Exception as e:
            # IMPORTANT: never crash the whole order details because addons schema differs
            addons = []

    # ---------------- Helpers ----------------
    def s(v):  # safe string
        return "" if v is None else str(v)

    def join_addr(a1, a2, city, state, pin):
        parts = [s(a1).strip(), s(a2).strip(), s(city).strip(), s(state).strip(), s(pin).strip()]
        return ", ".join([p for p in parts if p])

    store_address = join_addr(
        row.get("store_address_line1"),
        row.get("store_address_line2"),
        row.get("store_city"),
        row.get("store_state"),
        row.get("store_pincode"),
    )

    customer_address = join_addr(
        row.get("customer_address_line1"),
        row.get("customer_address_line2"),
        row.get("customer_city"),
        row.get("customer_state"),
        row.get("customer_pincode"),
    )

    # addons map
    addons_by_item = {}
    addons_total = 0.0
    for a in addons:
        oid = int(a["order_item_id"])
        price = float(a["price"] or 0)
        addons_total += price
        addons_by_item.setdefault(oid, []).append({
            "name": s(a.get("addon_name")),
            "price": price
        })

    # items block
    item_count = 0
    items_out = []
    for it in items:
        qty = int(it["qty"] or 0)
        unit_price = float(it["unit_price"] or 0)
        item_count += qty

        is_veg = it.get("is_veg")
        veg_flag = None
        if is_veg is not None:
            veg_flag = str(is_veg).strip().lower() in ("1", "true", "yes")

        items_out.append({
            "orderItemId": it["order_item_id"],
            "menuItemId": it.get("menu_item_id"),
            "name": s(it.get("item_name_snapshot")),
            "variant": s(it.get("variant_name_snapshot")),
            "unitPrice": unit_price,
            "qty": qty,
            "lineTotal": float(it.get("line_total") or (unit_price * qty)),
            "veg": veg_flag,
            "addons": addons_by_item.get(int(it["order_item_id"]), [])
        })

    # summary (from orders table)
    subtotal = float(row["subtotal"] or 0)
    packing = float(row["packing_charge"] or 0)
    delivery_fee = float(row["delivery_fee"] or 0)
    discount = float(row["discount_amount"] or 0)
    gst = float(row["tax_amount"] or 0)
    grand_total = float(row["grand_total"] or 0)

    # missing in DB -> 0 for now
    platform_fee = 0.0
    payment_gateway_charges = 0.0

    return jsonify({
        # ✅ General info block
        "generalInfo": {
            "orderId": row["order_id"],
            "orderNumber": s(row.get("order_number")),
            "date": row["created_at"].isoformat() if row.get("created_at") else "",
            "status": s(row.get("order_status")),
            "paymentType": s(row.get("payment_method")),
            "paymentStatus": s(row.get("payment_status")),
            "itemCount": item_count,
        },

        # ✅ Items info block
        "itemsInfo": items_out,

        # ✅ Order proof block (no columns yet)
        "orderProof": {
            "proofImageUrl": "",
            "notes": s(row.get("notes")),
        },

        # ✅ Deliveryman (rider) block
        "deliveryMan": {
            "riderId": row.get("rider_id"),
            "name": s(row.get("rider_name")),
            "zone": s(row.get("store_zone")),  # from stores.zone
        },

        # ✅ Delivery details block
        "deliveryDetails": {
            "fromStore": {
                "storeId": row["store_id"],
                "storeName": s(row.get("store_name")),
                "storeAddress": store_address,
                "storePhone": s(row.get("store_phone")),
            },
            "toCustomer": {
                "customerId": row["customer_id"],
                "customerName": s(row.get("customer_name")),
                "customerPhone": s(row.get("customer_phone")),
                "addressId": row.get("delivery_address_id"),
                "label": s(row.get("address_label")),
                "address": customer_address,
                "lat": row.get("latitude"),
                "lng": row.get("longitude"),
            }
        },

        # ✅ Restaurant details block
        "restaurantDetails": {
            "storeId": row["store_id"],
            "storeName": s(row.get("store_name")),
            "address": store_address,
            "logoUrl": s(row.get("store_logo_url")),
            "zone": s(row.get("store_zone")),
        },

        # ✅ Order summary block
        "orderSummary": {
            "price": round(subtotal, 2),
            "discount": round(discount, 2),
            "addons": round(addons_total, 2),
            "subtotal": round(subtotal, 2),
            "platformFee": round(platform_fee, 2),
            "gst": round(gst, 2),
            "deliveryFee": round(delivery_fee, 2),
            "packingCharge": round(packing, 2),
            "paymentGatewayCharges": round(payment_gateway_charges, 2),
            "totalAmount": round(grand_total, 2),
        }
    }), 200

# ======================
# vendor
# ======================
@app.route("/vendor/login", methods=["POST"])
def vendor_login_phone():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    password = str(data.get("password") or "").strip()

    if not phone or not password:
        return jsonify({"error": "phone and password required"}), 400

    with engine.connect() as conn:
        v = conn.execute(text("""
            SELECT vendor_id, name, email, phone, password_hash, status
            FROM vendors
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not v:
        return jsonify({"error": "Invalid credentials"}), 401

    if str(v.get("status") or "") != "ACTIVE":
        return jsonify({"error": "Vendor blocked"}), 403

    # ✅ your table currently stores plain values like 1111, 2020 (NOT bcrypt)
    if str(v.get("password_hash") or "") != password:
        return jsonify({"error": "Invalid credentials"}), 401

    # return vendor + stores list
    with engine.connect() as conn:
        stores = conn.execute(text("""
            SELECT * FROM stores
            WHERE vendor_id = :vid
            ORDER BY store_id DESC
        """), {"vid": int(v["vendor_id"])}).mappings().all()

    out_stores = []
    for s in stores:
        d = dict(s)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["cover_url"] = resolve_image_url(d.get("cover_url"))
        d["featured"] = int(d.get("is_featured") or 0)
        d["address"] = " ".join([x for x in [
            d.get("address_line1"), d.get("address_line2"),
            d.get("city"), d.get("state"), d.get("pincode")
        ] if x])
        out_stores.append(d)

    return jsonify({
        "ok": True,
        "vendor": {
            "vendor_id": int(v["vendor_id"]),
            "name": v.get("name") or "",
            "email": v.get("email") or "",
            "phone": v.get("phone") or "",
        },
        "stores": out_stores
    }), 200

@app.route("/vendor/update-profile", methods=["PUT"])
def vendor_update_profile():
    data = request.json or {}

    vendor_id = safe_int(data.get("vendor_id", 0), 0)
    name  = (data.get("name") or "").strip()
    phone = (data.get("phone") or "").strip()
    email = (data.get("email") or "").strip()

    if not vendor_id:
        return jsonify({"error": "vendor_id is required"}), 400

    updates = {}
    if name:
        updates["name"] = name
    if phone:
        updates["phone"] = phone
    if email:
        updates["email"] = email

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["vid"] = vendor_id

    # ✅ OPTIONAL: avoid duplicate phone/email for other vendors
    with engine.begin() as conn:
        if phone:
            dup = conn.execute(text("""
                SELECT vendor_id FROM vendors
                WHERE phone = :ph AND vendor_id <> :vid
                LIMIT 1
            """), {"ph": phone, "vid": vendor_id}).mappings().first()
            if dup:
                return jsonify({"error": "Phone already used by another vendor"}), 409

        if email:
            dup = conn.execute(text("""
                SELECT vendor_id FROM vendors
                WHERE email = :em AND vendor_id <> :vid
                LIMIT 1
            """), {"em": email, "vid": vendor_id}).mappings().first()
            if dup:
                return jsonify({"error": "Email already used by another vendor"}), 409

        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "vid"])
        res = conn.execute(text(f"""
            UPDATE vendors
            SET {set_clause}
            WHERE vendor_id = :vid
        """), updates)

        if res.rowcount == 0:
            return jsonify({"error": "Vendor not found"}), 404

        row = conn.execute(text("""
            SELECT vendor_id, name, email, phone, status
            FROM vendors
            WHERE vendor_id = :vid
            LIMIT 1
        """), {"vid": vendor_id}).mappings().first()

    return jsonify({
        "ok": True,
        "vendor": dict(row)
    }), 200

@app.route("/vendor/change-password", methods=["PUT"])
def vendor_change_password():
    data = request.json or {}

    vendor_id = safe_int(data.get("vendor_id", 0), 0)
    current_password = (data.get("current_password") or "").strip()
    new_password = (data.get("new_password") or "").strip()

    if not vendor_id or not current_password or not new_password:
        return jsonify({"error": "All fields required"}), 400

    with engine.begin() as conn:
        vendor = conn.execute(text("""
            SELECT password_hash FROM vendors
            WHERE vendor_id = :vid
            LIMIT 1
        """), {"vid": vendor_id}).mappings().first()

        if not vendor:
            return jsonify({"error": "Vendor not found"}), 404

        if str(vendor["password_hash"]) != current_password:
            return jsonify({"error": "Current password incorrect"}), 401

        conn.execute(text("""
            UPDATE vendors
            SET password_hash = :newp
            WHERE vendor_id = :vid
        """), {"newp": new_password, "vid": vendor_id})

    return jsonify({"ok": True}), 200

@app.route("/vendor/reset-password", methods=["PUT"])
def vendor_reset_password():
    data = request.json or {}

    phone = (data.get("phone") or "").strip()
    new_password = (data.get("new_password") or "").strip()

    if not phone or not new_password:
        return jsonify({"error": "phone and new_password required"}), 400

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT vendor_id FROM vendors
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"error": "Vendor not found"}), 404

        conn.execute(text("""
            UPDATE vendors
            SET password_hash = :pw
            WHERE phone = :ph
        """), {"pw": new_password, "ph": phone})

    return jsonify({"ok": True}), 200

@app.route("/vendor/orders/<int:store_id>", methods=["GET"])
def vendor_orders(store_id):
    with engine.connect() as conn:
        orders_rows = conn.execute(text("""
            SELECT
              o.order_id, o.order_number, o.created_at, o.order_status, o.grand_total,
              c.name AS customer_name, c.phone AS phone_number,
              ca.address_line1 AS address
            FROM orders o
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            WHERE o.store_id = :sid
            ORDER BY o.order_id DESC
        """), {"sid": store_id}).mappings().all()

        items_rows = conn.execute(text("""
            SELECT order_id,
                   item_name_snapshot AS product_name,
                   unit_price AS price,
                   qty AS quantity
            FROM order_items
            WHERE order_id IN (
                SELECT order_id FROM orders WHERE store_id = :sid
            )
            ORDER BY order_id DESC
        """), {"sid": store_id}).mappings().all()

    items_by_order = {}
    for it in items_rows:
        items_by_order.setdefault(int(it["order_id"]), []).append({
            "product_name": it.get("product_name", ""),
            "price": float(it.get("price") or 0),
            "quantity": int(it.get("quantity") or 0),
        })

    out = []
    for o in orders_rows:
        out.append({
            "id": int(o["order_id"]),
            "order_number": o.get("order_number") or "",
            "date": o["created_at"].strftime("%Y-%m-%d %H:%M") if o.get("created_at") else "",
            "status": str(o.get("order_status") or ""),
            "type": "Delivery",
            "total": float(o.get("grand_total") or 0),
            "items": items_by_order.get(int(o["order_id"]), []),
            "customer": {
                "name": o.get("customer_name") or "",
                "address": o.get("address") or "",
                "phone": o.get("phone_number") or ""
            }
        })

    return jsonify(out), 200

@app.route("/admin/item-details/<int:menu_item_id>", methods=["GET"])
def admin_item_details(menu_item_id):
    with engine.connect() as conn:
        item = conn.execute(text("""
            SELECT
              mi.menu_item_id,
              mi.store_id,
              mi.name,
              mi.description_short,
              mi.image_url,
              mi.status,
              mi.is_veg,
              mi.is_egg
            FROM menu_items mi
            WHERE mi.menu_item_id = :mid
            LIMIT 1
        """), {"mid": menu_item_id}).mappings().first()

        if not item:
            return jsonify({"error": "Item not found"}), 404

        variants = conn.execute(text("""
            SELECT variant_id, variant_name, price, mrp, status
            FROM menu_item_variants
            WHERE menu_item_id = :mid
            ORDER BY variant_id ASC
        """), {"mid": menu_item_id}).mappings().all()

    d = dict(item)
    d["image_url"] = resolve_image_url(d.get("image_url"))

    return jsonify({
        "ok": True,
        "item": {
            "menu_item_id": int(d["menu_item_id"]),
            "store_id": int(d["store_id"]),
            "name": d.get("name") or "",
            "short_description": d.get("description_short") or "",
            "image_url": d.get("image_url") or "",
            "status": "active" if str(d.get("status")) == "ACTIVE" else "inactive",
            "is_veg": int(d.get("is_veg") or 0),
            "is_egg": int(d.get("is_egg") or 0),
        },
        "variants": [
            {
                "variant_id": int(v["variant_id"]),
                "variant_name": v.get("variant_name") or "",
                "price": float(v.get("price") or 0),
                "mrp": float(v.get("mrp") or v.get("price") or 0),
                "status": str(v.get("status") or "ACTIVE"),
            }
            for v in variants
        ]
    }), 200

@app.route("/vendor/store/<int:store_id>", methods=["GET"])
def vendor_get_store(store_id):
    return get_store_by_id(store_id)   # reuse admin get-store


@app.route("/vendor/store/<int:store_id>", methods=["PUT"])
def vendor_update_store(store_id):
    return update_store(store_id)      # reuse admin update-store

@app.route("/update-order-status/<int:order_id>", methods=["PUT"])
def update_order_status(order_id):
    data = request.json or {}
    new_status = str(data.get("status") or "").strip()

    if not new_status:
        return jsonify({"error": "Status not provided"}), 400

    # map old statuses to new enum
    m = {
        "pending": "PLACED",
        "placed": "PLACED",
        "accepted": "ACCEPTED",
        "preparing": "PREPARING",
        "ready": "READY",
        "picked_up": "PICKED_UP",
        "delivered": "DELIVERED",
        "cancelled": "CANCELLED",
        "rejected": "REJECTED",
    }
    st = m.get(new_status.strip().lower(), new_status.strip().upper())

    with engine.begin() as conn:
        res = conn.execute(
            text("UPDATE orders SET order_status = :st WHERE order_id = :oid"),
            {"st": st, "oid": order_id}
        )
        if res.rowcount == 0:
            return jsonify({"error": "Order not found"}), 404

    return jsonify({"message": f"Order {order_id} status updated to {st}"}), 200

@app.route("/vendor/notifications", methods=["GET"])
def vendor_list_notifications():
    vendor_id = request.args.get("vendor_id", type=int)
    store_id = request.args.get("store_id", type=int)
    zone_id = request.args.get("zoneId", type=int)
    limit = request.args.get("limit", default=50, type=int)

    if not vendor_id and not store_id:
        return jsonify({"error": "vendor_id or store_id is required"}), 400

    with engine.connect() as conn:
        # get vendor stores first
        store_rows = conn.execute(text("""
            SELECT s.store_id, s.vendor_id, z.zone_id
            FROM stores s
            LEFT JOIN zones z
              ON s.zone COLLATE utf8mb4_unicode_ci = z.name COLLATE utf8mb4_unicode_ci
            WHERE (:vendor_id IS NULL OR s.vendor_id = :vendor_id)
              AND (:store_id IS NULL OR s.store_id = :store_id)
        """), {
            "vendor_id": vendor_id,
            "store_id": store_id
        }).mappings().all()

        if not store_rows:
            return jsonify([]), 200

        vendor_store_ids = [int(r["store_id"]) for r in store_rows]
        vendor_zone_ids = list({int(r["zone_id"]) for r in store_rows if r.get("zone_id") is not None})

        # optional extra zone filter from request
        if zone_id is not None:
            vendor_zone_ids = [zid for zid in vendor_zone_ids if zid == zone_id]
            if not vendor_zone_ids and not store_id:
                return jsonify([]), 200

        rows = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            WHERE n.status = 'SENT'
              AND (
                    LOWER(COALESCE(n.target, '')) = 'store'
                    OR UPPER(COALESCE(n.target_type, '')) = 'STORE'
                  )
            ORDER BY n.notification_id DESC
            LIMIT :lim
        """), {"lim": limit}).mappings().all()

    result = []
    vendor_store_ids_set = set(vendor_store_ids)
    vendor_zone_ids_set = set(vendor_zone_ids)

    for r in rows:
        notif_zone_id = r.get("zone_id")
        raw_json = r.get("target_value_json")

        target_store_id = None
        if raw_json:
            try:
                tv = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                if isinstance(tv, dict):
                    target_store_id = tv.get("targetId")
                    if target_store_id is not None:
                        target_store_id = int(target_store_id)
            except Exception:
                target_store_id = None

        # if notification targets a specific store, show only to that store/vendor
        if target_store_id is not None:
            if target_store_id in vendor_store_ids_set:
                result.append(r)
            continue

        # if notification is zone based / generic STORE notification
        if notif_zone_id is None:
            result.append(r)
            continue

        if notif_zone_id in vendor_zone_ids_set:
            result.append(r)

    return jsonify([_notif_row_to_api(r) for r in result]), 200
    
# ======================
# ✅ RIDER CREATE (ADMIN)
# POST /admin/add-rider
# Saves all rider details into riders table
# Supports multipart/form-data + GCS image upload
# ======================

@app.route("/admin/add-rider", methods=["POST"])
def admin_add_rider():
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    # Frontend keys
    first = str(data.get("firstName") or "").strip()
    last  = str(data.get("lastName") or "").strip()
    name  = (first + " " + last).strip()

    email = str(data.get("email") or "").strip()
    phone = str(data.get("phone") or "").strip()
    password = str(data.get("password") or "").strip()

    deliveryman_type = str(data.get("deliverymanType") or "").strip()
    zone = str(data.get("zone") or "").strip()
    vehicle = str(data.get("vehicle") or "").strip()
    identity_number = str(data.get("identityNumber") or "").strip()
    identity_type = str(data.get("identityType") or "").strip()

    if not name or not phone or not password:
        return jsonify({"error": "firstName/lastName, phone and password are required"}), 400

    # ✅ Upload identity images (optional)
    id_front_path = None
    id_back_path = None
    try:
        if is_multipart and "identityFront" in request.files:
            f = request.files["identityFront"]
            if f and f.filename:
                id_front_path = upload_file_to_gcs(f, folder=f"riders/{phone}/identity")

        if is_multipart and "identityBack" in request.files:
            f = request.files["identityBack"]
            if f and f.filename:
                id_back_path = upload_file_to_gcs(f, folder=f"riders/{phone}/identity")

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    # ✅ Password hash (bcrypt)
    pw_hash = password  # store plain password (NO bcrypt)

    # ✅ Duplicate phone check
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT rider_id FROM riders WHERE phone = :ph LIMIT 1"),
            {"ph": phone}
        ).mappings().first()

    if existing:
        return jsonify({"error": "Rider phone already exists"}), 409

    # ✅ Insert everything into riders
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO riders
              (name, first_name, last_name, phone, email,
               rider_type, zone, vehicle,
               identity_number, identity_type,
               identity_front_url, identity_back_url,
               password_hash, status)
            VALUES
              (:name, :first_name, :last_name, :phone, :email,
               :rider_type, :zone, :vehicle,
               :identity_number, :identity_type,
               :front, :back,
               :pw, 'ACTIVE')
        """), {
            "name": name,
            "first_name": first or None,
            "last_name": last or None,
            "phone": phone,
            "email": email or None,
            "rider_type": deliveryman_type or None,
            "zone": zone or None,
            "vehicle": vehicle or None,
            "identity_number": identity_number or None,
            "identity_type": identity_type or None,
            "front": id_front_path or None,
            "back": id_back_path or None,
            "pw": pw_hash,
        })

        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"])

    return jsonify({
        "ok": True,
        "message": "Rider created",
        "rider_id": new_id,
        "identity_front_url": resolve_image_url(id_front_path) if id_front_path else "",
        "identity_back_url": resolve_image_url(id_back_path) if id_back_path else ""
    }), 201

# ======================
# ✅ ADMIN: GET ONE RIDER (for edit prefill)
# GET /admin/riders/<id>
# ✅ NOW RETURNS password (plain text) for your prefill requirement
# ======================
@app.route("/admin/riders/<int:rider_id>", methods=["GET"])
def admin_get_rider(rider_id):
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT
              rider_id, name, first_name, last_name, email, phone,
              rider_type, zone, vehicle,
              identity_number, identity_type,
              identity_front_url, identity_back_url,
              password_hash,
              status, created_at
            FROM riders
            WHERE rider_id = :id
            LIMIT 1
        """), {"id": rider_id}).mappings().first()

    if not r:
        return jsonify({"error": "Rider not found"}), 404

    d = dict(r)
    d["identity_front_url"] = resolve_image_url(d.get("identity_front_url"))
    d["identity_back_url"] = resolve_image_url(d.get("identity_back_url"))

    # ✅ send password for edit prefill
    d["password"] = d.get("password_hash") or ""

    return jsonify(d), 200


# ======================
# ✅ ADMIN: UPDATE RIDER
# PUT /admin/riders/<id>
# Supports multipart/form-data (images optional)
# If password not sent -> keep old password
# ✅ FIXES:
#   1) Actually runs UPDATE (your pasted code was missing UPDATE execution)
#   2) If password not provided -> not updated
#   3) Returns updated rider with password for prefill
# ======================
@app.route("/admin/riders/<int:rider_id>", methods=["PUT"])
def admin_update_rider(rider_id):
    is_multipart = request.content_type and "multipart/form-data" in request.content_type
    data = request.form if is_multipart else (request.json or {})

    # fields
    first = (data.get("firstName") or data.get("first_name") or "").strip()
    last  = (data.get("lastName") or data.get("last_name") or "").strip()
    name  = ((first + " " + last).strip()) or (data.get("name") or "").strip()

    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()

    rider_type = (data.get("deliverymanType") or data.get("rider_type") or "").strip()
    zone = (data.get("zone") or "").strip()
    vehicle = (data.get("vehicle") or "").strip()
    identity_number = (data.get("identityNumber") or data.get("identity_number") or "").strip()
    identity_type = (data.get("identityType") or data.get("identity_type") or "").strip()

    password = (data.get("password") or "").strip()  # optional

    # must exist
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT phone, password_hash FROM riders WHERE rider_id = :id LIMIT 1"),
            {"id": rider_id}
        ).mappings().first()

    if not existing:
        return jsonify({"error": "Rider not found"}), 404

    # ✅ Upload images (optional)
    id_front_path = None
    id_back_path = None
    try:
        if is_multipart and "identityFront" in request.files:
            f = request.files["identityFront"]
            if f and f.filename:
                id_front_path = upload_file_to_gcs(
                    f,
                    folder=f"riders/{phone or existing['phone']}/identity"
                )

        if is_multipart and "identityBack" in request.files:
            f = request.files["identityBack"]
            if f and f.filename:
                id_back_path = upload_file_to_gcs(
                    f,
                    folder=f"riders/{phone or existing['phone']}/identity"
                )
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"GCS upload failed: {str(e)}"}), 500

    updates = {}

    # ✅ Only set columns when values provided (prevents overwriting with "")
    if first != "":
        updates["first_name"] = first
    if last != "":
        updates["last_name"] = last
    if name != "":
        updates["name"] = name

    # allow clearing email by sending empty? (your old logic used email or None)
    # keep same behavior:
    updates["email"] = email or None

    if phone:
        # prevent duplicate phone
        with engine.connect() as conn:
            dup = conn.execute(text("""
                SELECT rider_id FROM riders
                WHERE phone = :ph AND rider_id <> :id
                LIMIT 1
            """), {"ph": phone, "id": rider_id}).mappings().first()
        if dup:
            return jsonify({"error": "Phone already used by another rider"}), 409
        updates["phone"] = phone

    # keep old behavior: set to None when empty
    updates["rider_type"] = rider_type or None
    updates["zone"] = zone or None
    updates["vehicle"] = vehicle or None
    updates["identity_number"] = identity_number or None
    updates["identity_type"] = identity_type or None

    if id_front_path:
        updates["identity_front_url"] = id_front_path
    if id_back_path:
        updates["identity_back_url"] = id_back_path

    # ✅ password update only if provided
    if password:
        updates["password_hash"] = password  # store plain password

    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    updates["id"] = rider_id
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys() if k != "id"])

    # ✅ IMPORTANT: use transaction + actually execute UPDATE
    with engine.begin() as conn:
        res = conn.execute(text(f"""
            UPDATE riders
            SET {set_clause}
            WHERE rider_id = :id
        """), updates)

        if res.rowcount == 0:
            return jsonify({"error": "Rider not found"}), 404

        r = conn.execute(text("""
            SELECT
              rider_id, name, first_name, last_name, email, phone,
              rider_type, zone, vehicle,
              identity_number, identity_type,
              identity_front_url, identity_back_url,
              password_hash,
              status, created_at
            FROM riders
            WHERE rider_id = :id
            LIMIT 1
        """), {"id": rider_id}).mappings().first()

    d = dict(r)
    d["identity_front_url"] = resolve_image_url(d.get("identity_front_url"))
    d["identity_back_url"] = resolve_image_url(d.get("identity_back_url"))

    # ✅ send password for prefill (as you requested)
    d["password"] = d.get("password_hash") or ""

    return jsonify(d), 200

# ======================
# ✅ ADMIN: LIST RIDERS
# GET /admin/riders
# returns: id, name, phone(contact), zone, orders, availability
# ======================
@app.route("/admin/riders", methods=["GET"])
def admin_list_riders():
    zone = (request.args.get("zone") or "").strip()

    where = []
    params = {}
    if zone and zone.lower() != "all":
        where.append("r.zone = :zone")
        params["zone"] = zone

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                r.rider_id,
                r.name,
                r.phone,
                COALESCE(r.zone, '') AS zone,
                CASE
                    WHEN UPPER(COALESCE(r.status,'')) = 'ACTIVE' THEN 'Available'
                    ELSE 'Unavailable'
                END AS availability,

                -- ✅ orders count (based on deliveries table if rider_id exists)
                COALESCE((
                    SELECT COUNT(*)
                    FROM deliveries d
                    WHERE d.rider_id = r.rider_id
                ), 0) AS totalOrders

            FROM riders r
            {where_sql}
            ORDER BY r.rider_id DESC
        """), params).mappings().all()

    out = []
    for r in rows:
        out.append({
            "id": int(r["rider_id"]),
            "name": r.get("name") or "",
            "contact": r.get("phone") or "",
            "zone": r.get("zone") or "",
            "totalOrders": int(r.get("totalOrders") or 0),
            "availability": r.get("availability") or "Unavailable",
        })

    return jsonify(out), 200


# ======================
# ✅ ADMIN: DELETE RIDER
# DELETE /admin/riders/<id>
# ======================
@app.route("/admin/riders/<int:rider_id>", methods=["DELETE"])
def admin_delete_rider(rider_id):
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM riders WHERE rider_id = :id"), {"id": rider_id})
        if res.rowcount == 0:
            return jsonify({"error": "Rider not found"}), 404
    return jsonify({"ok": True}), 200

# ======================
# ✅ RIDER LOGIN (PLAIN PASSWORD)
# POST /rider/login
# body: { "phone": "9988...", "password": "xxxx" }
# ======================
@app.route("/rider/login", methods=["POST"])
def rider_login():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    password = str(data.get("password") or "").strip()

    if not phone or not password:
        return jsonify({"error": "phone and password required"}), 400

    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT rider_id, name, phone, password_hash, status, zone, rider_type, vehicle
            FROM riders
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not r:
        return jsonify({"error": "Invalid credentials"}), 401

    if str(r.get("status") or "").strip().upper() != "ACTIVE":
        return jsonify({"error": "Rider blocked"}), 403

    # ✅ plain password match
    if str(r.get("password_hash") or "") != password:
        return jsonify({"error": "Invalid credentials"}), 401

    return jsonify({
        "ok": True,
        "rider": {
            "rider_id": int(r["rider_id"]),
            "name": r.get("name") or "",
            "phone": r.get("phone") or "",
            "zone": r.get("zone") or "",
            "rider_type": r.get("rider_type") or "",
            "vehicle": r.get("vehicle") or "",
        }
    }), 200

# ======================
# ✅ RIDER RESET PASSWORD (PLAIN)
# PUT /rider/reset-password
# body: { "phone": "...", "new_password": "..." }
# ======================
@app.route("/rider/reset-password", methods=["PUT"])
def rider_reset_password():
    data = request.json or {}
    phone = str(data.get("phone") or "").strip()
    new_password = str(data.get("new_password") or "").strip()

    if not phone or not new_password:
        return jsonify({"error": "phone and new_password required"}), 400

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT rider_id FROM riders
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

        if not row:
            return jsonify({"error": "Rider not found"}), 404

        conn.execute(text("""
            UPDATE riders
            SET password_hash = :pw
            WHERE phone = :ph
        """), {"pw": new_password, "ph": phone})

    return jsonify({"ok": True, "message": "Password reset successful"}), 200

# ======================
# ✅ RIDER: PROFILE
# GET /rider/profile?phone=xxxxxxxxxx
# returns: name, created_at, total_orders
# ======================
@app.route("/rider/profile", methods=["GET"])
def rider_profile():
    phone = (request.args.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "phone is required"}), 400

    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT
              rider_id, name, first_name, last_name,
              phone, email, created_at,
              identity_front_url, identity_back_url
            FROM riders
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not r:
        return jsonify({"error": "Rider not found"}), 404

    d = dict(r)
    d["identity_front_url"] = resolve_image_url(d.get("identity_front_url"))
    d["identity_back_url"]  = resolve_image_url(d.get("identity_back_url"))

    return jsonify(d), 200

@app.route("/rider/change-password", methods=["PUT"])
def rider_change_password():
    data = request.get_json(silent=True) or {}

    phone = str(data.get("phone") or "").strip()
    old_pw = str(data.get("old_password") or "").strip()
    new_pw = str(data.get("new_password") or "").strip()

    if not phone or not old_pw or not new_pw:
        return jsonify({"error": "phone, old_password, new_password are required"}), 400

    # ✅ find rider
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT rider_id, password_hash
            FROM riders
            WHERE phone = :ph
            LIMIT 1
        """), {"ph": phone}).mappings().first()

    if not r:
        return jsonify({"error": "Rider not found"}), 404

    # ✅ plain text match
    if (r.get("password_hash") or "") != old_pw:
        return jsonify({"error": "Old password is incorrect"}), 401

    # ✅ update + verify
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE riders
            SET password_hash = :np
            WHERE rider_id = :id
        """), {"np": new_pw, "id": r["rider_id"]})

        # ✅ if nothing updated -> something wrong
        if res.rowcount == 0:
            return jsonify({
                "error": "Password not updated (rowcount=0)",
                "debug": {"phone": phone, "rider_id": int(r["rider_id"])}
            }), 500

        # ✅ verify
        verify = conn.execute(text("""
            SELECT password_hash
            FROM riders
            WHERE rider_id = :id
            LIMIT 1
        """), {"id": r["rider_id"]}).mappings().first()

    if not verify or (verify.get("password_hash") or "") != new_pw:
        return jsonify({"error": "Password update failed (verification failed)"}), 500

    return jsonify({"ok": True, "message": "Password updated successfully"}), 200

# ======================
# RIDER ROUTES (based on orders table)
# ======================

def fetch_orders_items_stores():
    with engine.connect() as conn:
        orders_rows = conn.execute(text("""
            SELECT
            o.order_id, o.store_id, o.rider_id, o.created_at, o.order_status, o.grand_total,
            c.name AS customer_name, c.phone AS phone_number,
            ca.address_line1 AS address
            FROM orders o
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            ORDER BY o.order_id DESC
        """)).mappings().all()

        items_rows = conn.execute(text("""
            SELECT
              order_id,
              item_name_snapshot AS product_name,
              unit_price AS price,
              qty AS quantity
            FROM order_items
            ORDER BY order_id DESC
        """)).mappings().all()

        store_rows = conn.execute(text("""
            SELECT store_id, store_name, address_line1, city
            FROM stores
        """)).mappings().all()

    items_by_order = {}
    for it in items_rows:
        items_by_order.setdefault(int(it["order_id"]), []).append(dict(it))

    store_by_id = {}
    for s in store_rows:
        d = dict(s)
        d["address"] = " ".join([x for x in [d.get("address_line1"), d.get("city")] if x])
        store_by_id[int(d["store_id"])] = d

    return orders_rows, items_by_order, store_by_id


def build_rider_order(order_row, items_by_order, store_by_id):
    sid = int(order_row.get("store_id", 0) or 0)
    store = store_by_id.get(sid, {})
    oid = int(order_row["order_id"])
    items = items_by_order.get(oid, [])

    return {
        "id": oid,
        "date": order_row["created_at"].strftime("%Y-%m-%d %H:%M") if order_row.get("created_at") else "",
        "status": str(order_row.get("order_status", "")),  # keep old key
        "total": float(order_row.get("grand_total", 0) or 0),

        "store_name": store.get("store_name", ""),
        "store_location": store.get("address", ""),

        "customer_name": str(order_row.get("customer_name", "")),
        "phone_number": str(order_row.get("phone_number", "")),
        "customer_address": str(order_row.get("address", "")),

        "item_count": int(len(items)),
        "items": items,
    }


def _norm_db_status(v) -> str:
    # ✅ normalize DB status safely (caps/spaces/null)
    return str(v or "").strip().upper()


@app.route("/rider/get-orders", methods=["GET"])
def rider_get_orders():
    orders_rows, items_by_order, store_by_id = fetch_orders_items_stores()
    out = []

    for o in orders_rows:
        st = _norm_db_status(o.get("order_status"))
        # ✅ rider pending = only PLACED
        if st != "PLACED":
            continue

        out.append(build_rider_order(o, items_by_order, store_by_id))

    return jsonify(out), 200

@app.route("/rider/accept-order", methods=["POST"])
def rider_accept_order():
    data = request.json or {}
    order_id = safe_int(data.get("order_id", 0), 0)
    rider_id = safe_int(data.get("rider_id", 0), 0)

    if not order_id or not rider_id:
        return jsonify({"error": "order_id and rider_id are required"}), 400

    with engine.begin() as conn:
        # ✅ Atomic claim: only 1 rider can win
        res = conn.execute(text("""
            UPDATE orders
            SET rider_id = :rid,
                order_status = 'ACCEPTED',
                updated_at = NOW()
            WHERE order_id = :oid
              AND (rider_id IS NULL OR rider_id = 0)
              AND UPPER(TRIM(order_status)) = 'PLACED'
        """), {"rid": rider_id, "oid": order_id})

        if res.rowcount == 0:
            # Someone else already accepted or order not in PLACED
            current = conn.execute(text("""
                SELECT order_id, rider_id, order_status
                FROM orders
                WHERE order_id = :oid
                LIMIT 1
            """), {"oid": order_id}).mappings().first()

            if not current:
                return jsonify({"error": "Order not found"}), 404

            return jsonify({
                "error": "Order already accepted / not available",
                "order": {
                    "order_id": int(current["order_id"]),
                    "rider_id": current.get("rider_id"),
                    "order_status": current.get("order_status"),
                }
            }), 409

    return jsonify({"ok": True, "message": "Order accepted", "order_id": order_id, "rider_id": rider_id}), 200

@app.route("/rider/get-active-orders", methods=["GET"])
def rider_get_active_orders():
    rider_id = safe_int(request.args.get("rider_id", 0), 0)
    if not rider_id:
        return jsonify({"error": "rider_id is required"}), 400

    with engine.connect() as conn:
        orders_rows = conn.execute(text("""
            SELECT
              o.order_id, o.store_id, o.rider_id, o.created_at, o.order_status, o.grand_total,
              c.name AS customer_name, c.phone AS phone_number,
              ca.address_line1 AS address
            FROM orders o
            LEFT JOIN customers c ON c.customer_id = o.customer_id
            LEFT JOIN customer_addresses ca ON ca.address_id = o.delivery_address_id
            WHERE o.rider_id = :rid
              AND UPPER(TRIM(o.order_status)) IN ('ACCEPTED','PREPARING','READY','PICKED_UP')
            ORDER BY o.order_id DESC
        """), {"rid": rider_id}).mappings().all()

        items_rows = conn.execute(text("""
            SELECT order_id, item_name_snapshot AS product_name, unit_price AS price, qty AS quantity
            FROM order_items
            ORDER BY order_id DESC
        """)).mappings().all()

        store_rows = conn.execute(text("""
            SELECT store_id, store_name, address_line1, city
            FROM stores
        """)).mappings().all()

    items_by_order = {}
    for it in items_rows:
        items_by_order.setdefault(int(it["order_id"]), []).append(dict(it))

    store_by_id = {}
    for s in store_rows:
        d = dict(s)
        d["address"] = " ".join([x for x in [d.get("address_line1"), d.get("city")] if x])
        store_by_id[int(d["store_id"])] = d

    out = [build_rider_order(o, items_by_order, store_by_id) for o in orders_rows]
    return jsonify(out), 200
    
@app.route("/rider/get-delivered-orders", methods=["GET"])
def rider_get_delivered_orders():
    rider_id = safe_int(request.args.get("rider_id", 0), 0)
    if not rider_id:
        return jsonify({"error": "rider_id is required"}), 400

    orders_rows, items_by_order, store_by_id = fetch_orders_items_stores()
    out = []

    for o in orders_rows:
        st = _norm_db_status(o.get("order_status"))
        if st != "DELIVERED":
            continue

        if safe_int(o.get("rider_id", 0), 0) != rider_id:
            continue

        out.append(build_rider_order(o, items_by_order, store_by_id))

    return jsonify(out), 200

@app.route("/rider/notifications", methods=["GET"])
def rider_list_notifications():
    rider_id = request.args.get("rider_id", type=int)
    phone = (request.args.get("phone") or "").strip()
    zone_id = request.args.get("zoneId", type=int)
    limit = request.args.get("limit", default=50, type=int)

    if not rider_id and not phone:
        return jsonify({"error": "rider_id or phone is required"}), 400

    with engine.connect() as conn:
        # ✅ find rider
        rider = None

        if rider_id:
            rider = conn.execute(text("""
                SELECT
                    r.rider_id,
                    r.phone,
                    r.zone,
                    z.zone_id
                FROM riders r
                LEFT JOIN zones z
                  ON r.zone COLLATE utf8mb4_unicode_ci = z.name COLLATE utf8mb4_unicode_ci
                WHERE r.rider_id = :rid
                LIMIT 1
            """), {"rid": rider_id}).mappings().first()
        else:
            rider = conn.execute(text("""
                SELECT
                    r.rider_id,
                    r.phone,
                    r.zone,
                    z.zone_id
                FROM riders r
                LEFT JOIN zones z
                  ON r.zone COLLATE utf8mb4_unicode_ci = z.name COLLATE utf8mb4_unicode_ci
                WHERE r.phone = :ph
                LIMIT 1
            """), {"ph": phone}).mappings().first()

        if not rider:
            return jsonify([]), 200

        rider_id_db = int(rider["rider_id"])
        rider_zone_id = rider.get("zone_id")

        # optional external zone filter
        if zone_id is not None and rider_zone_id is not None and int(zone_id) != int(rider_zone_id):
            return jsonify([]), 200

        rows = conn.execute(text("""
            SELECT n.*, z.name AS zone_name
            FROM notifications n
            LEFT JOIN zones z ON z.zone_id = n.zone_id
            WHERE n.status = 'SENT'
              AND (
                    LOWER(COALESCE(n.target, '')) = 'deliveryman'
                    OR LOWER(COALESCE(n.target, '')) = 'rider'
                    OR UPPER(COALESCE(n.target_type, '')) = 'DELIVERYMAN'
                  )
            ORDER BY n.notification_id DESC
            LIMIT :lim
        """), {"lim": limit}).mappings().all()

    result = []

    for r in rows:
        notif_zone_id = r.get("zone_id")
        raw_json = r.get("target_value_json")

        target_rider_id = None

        # ✅ parse target_value_json if present
        # supports:
        # {"targetId": 5}
        # {"riderId": 5}
        # {"deliverymanId": 5}
        # [1,2,3]
        if raw_json:
            try:
                tv = json.loads(raw_json) if isinstance(raw_json, str) else raw_json

                if isinstance(tv, dict):
                    if tv.get("targetId") is not None:
                        target_rider_id = int(tv.get("targetId"))
                    elif tv.get("riderId") is not None:
                        target_rider_id = int(tv.get("riderId"))
                    elif tv.get("deliverymanId") is not None:
                        target_rider_id = int(tv.get("deliverymanId"))

                elif isinstance(tv, list):
                    rider_ids = set()
                    for x in tv:
                        try:
                            rider_ids.add(int(x))
                        except Exception:
                            pass

                    if rider_id_db in rider_ids:
                        result.append(r)
                    continue

            except Exception:
                target_rider_id = None

        # ✅ specific rider notification
        if target_rider_id is not None:
            if target_rider_id == rider_id_db:
                result.append(r)
            continue

        # ✅ generic notification without zone => show to all riders
        if notif_zone_id is None:
            result.append(r)
            continue

        # ✅ zone-based rider notification
        if rider_zone_id is not None and int(notif_zone_id) == int(rider_zone_id):
            result.append(r)

    return jsonify([_notif_row_to_api(r) for r in result]), 200

@app.route("/stores-with-items", methods=["GET"])
def stores_with_items():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              s.store_id,
              s.store_name,
              CONCAT_WS(' ', s.address_line1, s.address_line2, s.city, s.state, s.pincode) AS address,
              COALESCE(s.city, '') AS location,
              s.logo_url,
              s.status,
              (
                SELECT GROUP_CONCAT(mi.name ORDER BY mi.menu_item_id DESC SEPARATOR ', ')
                FROM menu_items mi
                WHERE mi.store_id = s.store_id
                LIMIT 3
              ) AS items_preview
            FROM stores s
            WHERE s.status = 'ACTIVE'
            ORDER BY s.store_id DESC
        """)).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["logo_url"] = resolve_image_url(d.get("logo_url"))
        d["status"] = True if str(d.get("status")) == "ACTIVE" else False
        d["items_preview"] = d.get("items_preview") or ""
        out.append(d)

    return jsonify(out), 200

# ======================
# RUN
# ======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
