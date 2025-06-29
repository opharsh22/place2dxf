from flask import Flask, request, jsonify, send_from_directory
import os, tempfile, logging, io, zipfile
import requests, geopandas as gpd, shapely, pyproj, ezdxf
from shapely.geometry import Polygon, MultiPolygon

# ── CONFIG ──────────────────────────────────────────────────────────────────────
OV_RELEASE      = "2025-06-25.0"
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
TARGET_CRS      = "EPSG:32644"          # Lucknow UTM
DEFAULT_BUFFER  = 250                   # metres
OVERPASS        = "https://overpass.kumi.systems/api/interpreter"
# ────────────────────────────────────────────────────────────────────────────────

# Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("place2dxf")

app = Flask(__name__)

# ── HELPERS ─────────────────────────────────────────────────────────────────────
def geocode(place: str) -> tuple[float, float]:
    r = requests.get("https://nominatim.openstreetmap.org/search",
                     params={"q": place, "format": "json", "limit": 1,
                             "email": NOMINATIM_EMAIL}, timeout=10)
    r.raise_for_status()
    lat, lon = float(r.json()[0]["lat"]), float(r.json()[0]["lon"])
    log.info("[GEO] %s ➜ %.6f, %.6f", place, lat, lon)
    return lat, lon

def overture_buildings_extract(bbox4326):
    minLon, minLat, maxLon, maxLat = bbox4326
    url = ("https://extract.overturemaps.org/extract.json"      # ← fixed host
           f"?bbox={minLon},{minLat},{maxLon},{maxLat}"
           "&layers=buildings")
    log.info("[API] %s", url)
    meta = requests.get(url, timeout=20).json()
    file_url = meta["layers"]["buildings"]["gpkg"]
    log.info("[DL ] %s", file_url.split('/')[-1])
    blob = requests.get(file_url, timeout=20).content
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            gdf = gpd.read_file(f)
    return gdf.to_crs(TARGET_CRS)

def overpass_roads(bbox4326):
    s, w, n, e = bbox4326[1], bbox4326[0], bbox4326[3], bbox4326[2]
    q = f"[out:json];way[highway]({s},{w},{n},{e});out geom;"
    r = requests.get(OVERPASS, params={"data": q}, timeout=20)
    r.raise_for_status()
    lines = [shapely.LineString([(p["lon"], p["lat"])
             for p in el["geometry"]])
             for el in r.json()["elements"]]
    log.info("[RES] roads rows=%d", len(lines))
    return gpd.GeoDataFrame(geometry=lines, crs=4326).to_crs(TARGET_CRS)

# ── ROUTES ──────────────────────────────────────────────────────────────────────
@app.route("/")
def hello():
    return "Service up — /dwg?place=Lucknow"

@app.route("/dwg")
def make_dxf():
    place = request.args.get("place", "").strip()
    if not place:
        return jsonify(error="missing ?place"), 400
    buf = float(request.args.get("buffer", DEFAULT_BUFFER))

    try:
        # 1  geocode & AOI
        lat, lon = geocode(place)
        x, y = pyproj.Transformer.from_crs(4326, TARGET_CRS, always_xy=True)\
                                 .transform(lon, lat)
        aoi_m = shapely.box(x-buf, y-buf, x+buf, y+buf)
        bbox  = gpd.GeoSeries([aoi_m], crs=TARGET_CRS)\
                  .to_crs(4326).total_bounds

        # 2  layers
        buildings = overture_buildings_extract(bbox)
        roads     = overpass_roads(bbox)
        water     = gpd.GeoDataFrame(columns=["geometry"], crs=TARGET_CRS)  # stub

        # 3  DXF
        tmp, fname = tempfile.gettempdir(), f"{place.replace(' ', '_')}.dxf"
        path = os.path.join(tmp, fname)
        doc  = ezdxf.new()
        msp  = doc.modelspace()
        for lyr in ("BLDG", "ROAD"):
            doc.layers.new(lyr)

        def add_poly(poly):
            msp.add_lwpolyline(list(poly.exterior.coords),
                               dxfattribs={"layer": "BLDG"})

        for g in buildings.geometry:
            if isinstance(g, Polygon):
                add_poly(g)
            elif isinstance(g, MultiPolygon):
                for p in g.geoms:
                    add_poly(p)

        for ln in roads.geometry:
            msp.add_lwpolyline(list(ln.coords), dxfattribs={"layer": "ROAD"})

        doc.saveas(path)
        log.info("[DXF] BLDG=%d  ROAD=%d", len(buildings), len(roads))
        return jsonify(status="ok", download_url=f"/files/{fname}")

    except Exception as ex:
        log.exception("[ERR] DXF generation failed")
        return jsonify(error=str(ex)), 500

@app.route("/files/<fname>")
def download(fname):
    return send_from_directory(tempfile.gettempdir(), fname, as_attachment=True)

# ── MAIN ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 3000)))
