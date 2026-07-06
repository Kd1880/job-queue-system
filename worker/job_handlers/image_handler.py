"""
worker/job_handlers/image_handler.py
------------------
PURPOSE: Two image job handlers:
  - handle_resize_image  (Phase 1, kept unchanged): simple multi-size
    aspect-preserving resize.
  - handle_image_processor (Phase 2): the full pipeline — EXIF metadata
    extraction, multi-size resize, format conversion (WebP), lossy
    compression, thumbnail generation, and size-reduction reporting, all
    from a SINGLE decode of the source image.

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher for job types
  "resize_image" / "image_processor". Both are deliberately synchronous
  (`def`, not `async def`) — Pillow has no async API and this is CPU-bound
  pixel work, not I/O-bound waiting. worker/worker.py runs them via
  `asyncio.to_thread` so the event loop stays free.

KEY CONCEPTS (Phase 2):
  EXIF — a metadata block cameras embed inside JPEG/HEIC files: camera
    model, capture timestamp, GPS coordinates, orientation. Invisible in
    the pixels, but travels with the file — which is why photo-sharing
    sites strip or process it (privacy: GPS in a posted photo leaks your
    home address).
  WebP — Google's image format: ~25-35% smaller than JPEG at equivalent
    visual quality thanks to better entropy coding + prediction. Every
    modern browser supports it; converting user uploads to WebP is
    standard practice at image-heavy companies (Meta, Pinterest, Shopify).
  ASPECT RATIO — width:height proportion. Forcing an image into a box
    with a different ratio distorts it (stretched faces); Pillow's
    .thumbnail() shrinks to FIT WITHIN the box, preserving the ratio.
  QUALITY — lossy encoders take a 1-100 knob trading bytes for fidelity.
    85 is the industry sweet spot: visually indistinguishable from 100
    for most photos at roughly half the file size.
"""

import os
from datetime import datetime

from PIL import Image, UnidentifiedImageError
from PIL.ExifTags import Base as ExifBase


def handle_resize_image(payload: dict) -> dict:
    """
    Resize an image to each requested size, preserving aspect ratio.

    ARGS:
      payload: {"image_path": str, "sizes": [[w, h], ...]} — already
               validated for shape/positivity by ImagePayload
               (api/models.py); file existence is checked here, at
               execution time.

    FLOW:
      1. Verify the source image exists.
      2. Open it with Pillow and record its original dimensions.
      3. For each requested (width, height):
           - Make a fresh copy of the original (thumbnail() mutates
             in-place, so reusing one Image object across sizes would
             compound each resize onto the previous one instead of always
             resizing from the true original).
           - Use .thumbnail((width, height)) — Pillow's aspect-ratio-
             preserving resize. It shrinks the image to fit WITHIN the
             given box, never upscaling and never distorting proportions
             (unlike .resize(), which would force the exact dimensions and
             stretch the image if the aspect ratio didn't match).
           - Save it as "{name}_{width}x{height}.{ext}" in resized/.
      4. Return the original size plus a list of every output file produced.

    RETURNS:
      {
        "original_size": [width, height],
        "output_files": [{"size": [w, h], "path": str}, ...],
      }
    """
    image_path = payload["image_path"]
    sizes = payload["sizes"]

    # Step 1: Fail fast with a clear message rather than letting Pillow
    # raise its own lower-level error for a missing file.
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image file not found: {image_path}")

    # Step 2: Open the source image once. Image.open() is lazy (doesn't
    # decode pixel data until needed), so this is cheap even before we
    # know we'll use it multiple times below.
    original_image = Image.open(image_path)
    original_size = list(original_image.size)  # (width, height) -> [width, height]

    # Prepare the output directory once, up front.
    output_dir = "resized"
    os.makedirs(output_dir, exist_ok=True)

    # Split "uploads/photo.jpg" into name="photo" and ext="jpg" so we can
    # build output filenames like "photo_800x600.jpg".
    filename = os.path.basename(image_path)
    name, ext = os.path.splitext(filename)
    ext = ext.lstrip(".")

    output_files = []

    # Step 3: Produce one resized copy per requested size.
    for width, height in sizes:
        # .copy() is essential here: Image.thumbnail() resizes IN PLACE.
        # Without copying, the second iteration would call thumbnail() on
        # an already-shrunk image instead of the original — producing
        # wrong (over-shrunk) results for every size after the first.
        resized_copy = original_image.copy()

        # thumbnail() preserves aspect ratio by shrinking the image to fit
        # within the (width, height) bounding box — it will never stretch
        # or distort the image the way a plain .resize(width, height)
        # would if the requested aspect ratio didn't match the original.
        resized_copy.thumbnail((width, height))

        output_path = os.path.join(output_dir, f"{name}_{width}x{height}.{ext}")
        resized_copy.save(output_path)

        output_files.append({"size": [width, height], "path": output_path})

    # Step 4: Return a summary of what was produced.
    return {
        "original_size": original_size,
        "output_files": output_files,
    }


# ============================================================================
# PHASE 2: FULL IMAGE PIPELINE (job type "image_processor")
# ============================================================================

def _extract_exif(image: Image.Image) -> dict:
    """
    Pull the interesting EXIF fields out of an image, if any exist.

    WHY .get() EVERYWHERE / WHY THIS NEVER RAISES: EXIF is optional and
    wildly inconsistent — screenshots have none, WhatsApp strips it,
    every camera vendor fills different fields. Metadata extraction is a
    bonus, not the job's purpose, so a missing/corrupt EXIF block must
    never fail an otherwise-valid image job. Worst case: empty dict.
    """
    exif_out: dict = {}
    try:
        # getexif() returns the base EXIF directory (IFD0) — always
        # present as an object, possibly empty.
        exif = image.getexif()
        if not exif:
            return exif_out

        # Camera make + model live in the base directory. Join whichever
        # of the two exists: "Apple iPhone 15" / "iPhone 15" / "Apple".
        make = exif.get(ExifBase.Make)
        model = exif.get(ExifBase.Model)
        camera = " ".join(part.strip() for part in (make, model) if part)
        if camera:
            exif_out["camera"] = camera

        # The capture timestamp lives in a NESTED directory (the "Exif
        # IFD", pointer tag 0x8769) — not in the base block. get_ifd()
        # follows that pointer. DateTimeOriginal = when the shutter
        # fired; base DateTime = when the file was last modified (edits
        # update it), so Original is the honest "taken at".
        exif_ifd = exif.get_ifd(0x8769)
        taken = exif_ifd.get(ExifBase.DateTimeOriginal) or exif.get(ExifBase.DateTime)
        if taken:
            try:
                # EXIF uses "2026:06:01 10:00:00" (colons in the DATE —
                # a 1990s spec quirk). Convert to standard ISO 8601 so
                # the stored result is directly machine-parseable.
                exif_out["taken_at"] = datetime.strptime(
                    str(taken), "%Y:%m:%d %H:%M:%S"
                ).isoformat()
            except ValueError:
                # Non-standard timestamp — store raw rather than lose it.
                exif_out["taken_at"] = str(taken)

        # GPS is another nested directory (pointer tag 0x8825). We only
        # report whether coordinates EXIST — good demo of why services
        # strip EXIF (privacy) without this result leaking locations into
        # our own Postgres.
        gps_ifd = exif.get_ifd(0x8825)
        if gps_ifd:
            exif_out["has_gps"] = True

    except Exception:
        # Corrupt EXIF block in an otherwise-fine image: ignore entirely.
        pass

    return exif_out


def _save_variant(image: Image.Image, path: str, fmt: str, quality: int) -> int:
    """
    Encode one output image to disk and return its size in bytes.

    fmt: Pillow format name ("WEBP", "JPEG", "PNG").
    quality: the 1-100 lossy-compression knob (ignored by PNG, which is
    lossless — Pillow just skips unknown kwargs per-format).
    """
    # JPEG can't store an alpha (transparency) channel — saving an RGBA
    # image as JPEG raises. Flatten to RGB first. WebP/PNG keep alpha.
    if fmt == "JPEG" and image.mode in ("RGBA", "P"):
        image = image.convert("RGB")

    # quality: how aggressively the lossy encoder throws away detail.
    # method=6 (WebP only): spend more CPU searching for a smaller
    # encoding — right tradeoff in a background worker where latency is
    # already paid for, unlike in a web request.
    save_kwargs: dict = {"quality": quality}
    if fmt == "WEBP":
        save_kwargs["method"] = 6

    image.save(path, format=fmt, **save_kwargs)
    return os.path.getsize(path)


def handle_image_processor(payload: dict) -> dict:
    """
    Run the full image pipeline: EXIF -> resize(s) -> convert/compress ->
    thumbnail, decoding the expensive source image only ONCE.

    ARGS:
      payload: validated by ImageProcessorPayload (api/models.py):
        {"image_path": str, "operations": [...],
         "resize_dimensions": [[w,h],...] | None, "convert_to": "webp",
         "quality": 85, "generate_thumbnail": bool, "thumbnail_size": [w,h]}

    RETURNS:
      {"original": {size_bytes, dimensions, format},
       "outputs": [{operation, path, size_bytes, size_reduction_percent}],
       "exif": {camera?, taken_at?, has_gps?}}
    """
    image_path = payload["image_path"]
    operations = payload["operations"]
    quality = payload.get("quality", 85)
    convert_to = payload.get("convert_to", "webp").lower()

    # Fail fast with a clear message for the retry/DLQ error history.
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image file not found: {image_path}")

    original_bytes = os.path.getsize(image_path)

    # UnidentifiedImageError = the bytes aren't a decodable image at all
    # (truncated upload, renamed .txt, corruption). Re-raise as ValueError
    # with a human message: this is a PERMANENT failure — retrying will
    # decode the same broken bytes — so it should march straight through
    # the retry counter into the DLQ with an obvious explanation attached.
    try:
        original_image = Image.open(image_path)
        # Image.open() is lazy — it only reads the header. load() forces
        # full pixel decode NOW, so corruption deeper in the file surfaces
        # here (inside our try) instead of exploding mid-resize below.
        original_image.load()
    except UnidentifiedImageError as exc:
        raise ValueError(f"File is not a valid image (corrupt?): {image_path}") from exc
    except OSError as exc:
        raise ValueError(f"Image is corrupt or truncated: {image_path} ({exc})") from exc

    original_info = {
        "size_bytes": original_bytes,
        "dimensions": list(original_image.size),  # (w, h) -> [w, h] for JSON
        "format": original_image.format,          # "JPEG" / "PNG" / ...
    }

    # EXIF must be read from the ORIGINAL image object — resized copies
    # produced below are new in-memory images that carry no metadata
    # (which is also why the outputs are automatically EXIF-stripped: a
    # privacy win for free).
    exif_data = _extract_exif(original_image)

    # Output format: only actually convert if the caller asked for the
    # "convert" stage; otherwise keep the original format. Uppercase is
    # Pillow's format-name convention; the extension stays lowercase.
    if "convert" in operations:
        out_fmt = convert_to.upper()
        out_ext = convert_to
    else:
        out_fmt = original_image.format or "PNG"
        out_ext = out_fmt.lower()

    output_dir = "resized"
    os.makedirs(output_dir, exist_ok=True)
    name, _ = os.path.splitext(os.path.basename(image_path))

    outputs = []

    # ---- RESIZE stage: one output per requested bounding box ----
    if "resize" in operations:
        resize_dimensions = payload.get("resize_dimensions")
        if not resize_dimensions:
            raise ValueError("'resize' operation requested but resize_dimensions is empty")

        for width, height in resize_dimensions:
            # .copy() because .thumbnail() mutates in place — without it,
            # the second box would shrink the already-shrunk first result
            # instead of the original (same pitfall as Phase 1's handler).
            variant = original_image.copy()
            # .thumbnail() = shrink-to-FIT-WITHIN (w,h) preserving aspect
            # ratio; never distorts, never upscales. .resize() would force
            # exact dimensions and stretch the image.
            variant.thumbnail((width, height))

            out_path = os.path.join(output_dir, f"{name}_{width}x{height}.{out_ext}")
            out_bytes = _save_variant(variant, out_path, out_fmt, quality)

            outputs.append({
                "operation": f"resize_{width}x{height}",
                "path": out_path,
                "size_bytes": out_bytes,
                # The headline number: "3840x2160 2.4MB JPEG -> 800x600
                # 45KB WebP = 98.1% smaller". Guard against a zero-byte
                # original (can't divide by zero).
                "size_reduction_percent": round((1 - out_bytes / original_bytes) * 100, 1)
                if original_bytes else 0.0,
            })

    # ---- CONVERT/COMPRESS-only path: full-size re-encode ----
    # If the caller wants conversion or recompression but no resizing,
    # produce ONE full-resolution copy in the target format/quality.
    # (When "resize" ran above, its outputs already ARE converted and
    # compressed — one decode, every stage applied per output.)
    if "resize" not in operations and ("convert" in operations or "compress" in operations):
        out_path = os.path.join(output_dir, f"{name}_full.{out_ext}")
        out_bytes = _save_variant(original_image.copy(), out_path, out_fmt, quality)
        outputs.append({
            "operation": "convert_full_size" if "convert" in operations else "compress",
            "path": out_path,
            "size_bytes": out_bytes,
            "size_reduction_percent": round((1 - out_bytes / original_bytes) * 100, 1)
            if original_bytes else 0.0,
        })

    # ---- THUMBNAIL stage ----
    # Triggered by either the operations list or the boolean flag — the
    # payload spec allows both spellings.
    if "thumbnail" in operations or payload.get("generate_thumbnail"):
        thumb_w, thumb_h = payload.get("thumbnail_size", (150, 150))
        thumb = original_image.copy()
        thumb.thumbnail((thumb_w, thumb_h))

        thumb_path = os.path.join(output_dir, f"{name}_thumb.{out_ext}")
        thumb_bytes = _save_variant(thumb, thumb_path, out_fmt, quality)
        outputs.append({
            "operation": "thumbnail",
            "path": thumb_path,
            "size_bytes": thumb_bytes,
        })

    return {
        "original": original_info,
        "outputs": outputs,
        "exif": exif_data,
    }
