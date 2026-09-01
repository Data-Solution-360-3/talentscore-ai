import io
import struct
import cairosvg

STATIC = "/home/topcandidate/app/static"

DETAILED = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect x="1" y="1" width="62" height="62" rx="14" fill="#EAF3DE"/>
  <circle cx="28" cy="23" r="8.6" fill="#639922"/>
  <path d="M12.5 49.8 C12.5 38.6 19.4 33 28 33 C36.6 33 43.5 38.6 43.5 49.8 Z" fill="#639922"/>
  <circle cx="46.5" cy="46.5" r="13" fill="#EAF3DE"/>
  <circle cx="46.5" cy="46.5" r="10.6" fill="#EF9F27"/>
  <path d="M41.2 46.8 L45 50.5 L52.2 42.6" fill="none" stroke="#ffffff" stroke-width="2.9" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""

SIMPLE = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect x="0" y="0" width="64" height="64" rx="13" fill="#639922"/>
  <circle cx="26" cy="22" r="11" fill="#EAF3DE"/>
  <path d="M7 52 C7 38 15.5 31 26 31 C36.5 31 45 38 45 52 Z" fill="#EAF3DE"/>
  <circle cx="47" cy="47" r="15" fill="#639922"/>
  <circle cx="47" cy="47" r="12" fill="#EF9F27"/>
  <path d="M40.5 47.3 L45 51.8 L54 41.6" fill="none" stroke="#ffffff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""


def png(svg, size):
    return cairosvg.svg2png(bytestring=svg.encode(), output_width=size, output_height=size)


# (size, png-bytes) — simplified reads cleanly small, detailed at 48
entries = [(16, png(SIMPLE, 16)), (32, png(SIMPLE, 32)), (48, png(DETAILED, 48))]

n = len(entries)
header = struct.pack("<HHH", 0, 1, n)  # reserved, type=icon, count
dir_entries = b""
offset = 6 + 16 * n
image_blob = b""
for size, data in entries:
    w = h = (0 if size >= 256 else size)
    dir_entries += struct.pack("<BBBBHHII", w, h, 0, 0, 1, 32, len(data), offset)
    image_blob += data
    offset += len(data)

with open(f"{STATIC}/favicon.ico", "wb") as f:
    f.write(header + dir_entries + image_blob)

# verify
from PIL import Image
im = Image.open(f"{STATIC}/favicon.ico")
print("ico sizes:", sorted(im.ico.sizes()))
print("bytes:", 6 + 16 * n + len(image_blob))
