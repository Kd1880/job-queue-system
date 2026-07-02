"""
worker/job_handlers/image_handler.py
------------------
PURPOSE: Executes a `resize_image` job — opens an image and produces
         multiple resized copies at requested dimensions.

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher whenever a job's
  `type` is "resize_image". A REAL (not mocked) implementation using
  Pillow. Deliberately synchronous (`def`, not `async def`) — Pillow has no
  async API and this is CPU-bound image processing, not I/O-bound network
  waiting. worker/worker.py runs this via `asyncio.to_thread` so it
  doesn't block the event loop while it runs.
"""

import os

from PIL import Image


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
