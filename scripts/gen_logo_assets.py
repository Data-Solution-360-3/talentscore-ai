import io
import cairosvg
from PIL import Image

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

OG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630">
  <rect width="1200" height="630" fill="#F4FAF0"/>
  <rect width="1200" height="10" fill="#EF9F27"/>
  <rect y="620" width="1200" height="10" fill="#639922"/>
  <g transform="translate(430 150) scale(4.6)">
    <rect x="1" y="1" width="62" height="62" rx="14" fill="#EAF3DE"/>
    <circle cx="28" cy="23" r="8.6" fill="#639922"/>
    <path d="M12.5 49.8 C12.5 38.6 19.4 33 28 33 C36.6 33 43.5 38.6 43.5 49.8 Z" fill="#639922"/>
    <circle cx="46.5" cy="46.5" r="13" fill="#EAF3DE"/>
    <circle cx="46.5" cy="46.5" r="10.6" fill="#EF9F27"/>
    <path d="M41.2 46.8 L45 50.5 L52.2 42.6" fill="none" stroke="#ffffff" stroke-width="2.9" stroke-linecap="round" stroke-linejoin="round"/>
  </g>
  <text x="602" y="490" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-size="58" font-weight="bold" fill="#1f2937">TopCandidate</text>
  <text x="606" y="490" text-anchor="start" font-family="DejaVu Sans, Arial, sans-serif" font-size="58" font-weight="bold" fill="#EF9F27">.pro</text>
  <text x="600" y="545" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="27" fill="#4B5563">Every candidate, scored on your rubric.</text>
</svg>"""


def render(svg, size):
    png = cairosvg.svg2png(bytestring=svg.encode(), output_width=size, output_height=size)
    return Image.open(io.BytesIO(png)).convert("RGBA")


# Large icons — detailed mark
for name, sz in [("icon-512.png", 512), ("logo-mark-512.png", 512),
                 ("favicon-192.png", 192), ("favicon-96.png", 96),
                 ("apple-touch-icon.png", 180), ("favicon-48.png", 48)]:
    render(DETAILED, sz).save(f"{STATIC}/{name}")
    print("wrote", name)

# favicon.ico — SIMPLIFIED at 16/32 (legible), DETAILED at 48
ico16 = render(SIMPLE, 16)
ico32 = render(SIMPLE, 32)
ico48 = render(DETAILED, 48)
ico16.save(f"{STATIC}/favicon.ico", format="ICO",
           sizes=[(16, 16), (32, 32), (48, 48)], append_images=[ico32, ico48])
print("wrote favicon.ico (16 simple, 32 simple, 48 detailed)")

# OG share image
og_png = cairosvg.svg2png(bytestring=OG.encode(), output_width=1200, output_height=630)
Image.open(io.BytesIO(og_png)).convert("RGB").save(f"{STATIC}/og-image.png")
print("wrote og-image.png")

# Zoomed 16px previews (nearest-neighbour blow-up) for the mush test
render(SIMPLE, 16).resize((192, 192), Image.NEAREST).save("/tmp/fav16_simple_zoom.png")
render(DETAILED, 16).resize((192, 192), Image.NEAREST).save("/tmp/fav16_detailed_zoom.png")
print("wrote 16px zoom previews to /tmp")
