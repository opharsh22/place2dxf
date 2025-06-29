from flask import Flask, request, jsonify, send_from_directory
import os, tempfile, logging, io, zipfile, requests, duckdb, geopandas as gpd, pandas as pd
import shapely, pyproj, ezdxf
from shapely.geometry import Polygon, MultiPolygon

# ── CONFIG ──────────────────────────────────────────────────────────────────────
OV_RELEASE      = "2025-06-25.0"
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
TARGET_CRS      = "EPSG:32644"
DEFAULT_BUFFER  = 250
OVERPASS        = "https://overpass.kumi.systems/api/interpreter"
# ────────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("place2dxf")

app = Flask(__name__)

# ── helpers ────────────────────────────────────────────────────────────────────
def geocode(place):
    r = requests.get("https://nominatim.openstreetmap.org/search",
                     params={"q": place, "format": "json", "limit": 1,
                             "email": NOMINATIM_EMAIL}, timeout=10)
    r.raise_for_status()
    lat, lon = float(r.json()[0]["lat"]), float(r.json()[0]["lon"])
    log.info("[GEO] %s → %.6f, %.6f", place, lat, lon)
    return lat, lon

def extract_buildings_api(bbox):
    minLon, minLat, maxLon, maxLat = bbox
    url = (f"https://extract.overturemaps.org/extract.json"
           f"?bbox={minLon},{minLat},{maxLon},{maxLat}&layers=buildings")
    log.info("[API] %s", url)
    meta = requests.get(url, timeout=20).json()
    zip_url = meta["layers"]["buildings"]["gpkg"]
    log.info("[DL ] %s", zip_url.split("/")[-1])
    blob = requests.get(zip_url, timeout=30).content
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            gdf = gpd.read_file(f)
    return gdf.to_crs(TARGET_CRS)

def extract_buildings_parquet(bbox):
    xmin, ymin, xmax, ymax = bbox
    box_sql = f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})"
    url = (f"s3://overturemaps-us-west-2/release/{OV_RELEASE}/"
           f"theme=buildings/type=building/**")
    log.info("[SLOW] parquet scan buildings …")
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    df = con.sql(f"""
        SELECT *, ST_GeomFromWKB(geometry) AS geom
        FROM parquet_scan('{url}')
        WHERE ST_Intersects(ST_GeomFromWKB(geometry), {box_sql})
    """).fetchdf()
    con.close()
    if df.empty:
        return gpd.GeoDataFrame(columns=["geometry"], crs=4326).to_crs(TARGET_CRS)
    df = df.drop(columns=["geometry"]).rename(columns={"geom": "geometry"})
    return gpd.GeoDataFrame(df, geometry="geometry", crs=4326).to_crs(TARGET_CRS)

def get_buildings(bbox):
    try:
        return extract_buildings_api(bbox)
    except Exception as e:
        log.warning("[WARN] extract API failed (%s) — falling back to Parquet", e.__class__.__name__)
        return extract_buildings_parquet(bbox)

def overpass_roads(bbox):
    s, w, n, e = bbox[1], bbox[0], bbox[3], bbox[2]
    q = f"[out:json];way[highway]({s},{w},{n},{e});out geom;"
    r = requests.get(OVERPASS, params={"data": q}, timeout=25)
    r.raise_for_status()
    lines = [shapely.LineString([(p["lon"], p["lat"]) for p in el["geometry"]])
             for el in r.json()["elements"]]
    log.info("[RES] roads %d", len(lines))
    return gpd.GeoDataFrame(geometry=lines, crs=4326).to_crs(TARGET_CRS)

# ── route ──────────────────────────────────────────────────────────────────────
@app.route("/dwg")
def make_dxf():
    place = request.args.get("place", "").strip()
    if not place:
        return jsonify(error="missing ?place"), 400
    buf = float(request.args.get("buffer", DEFAULT_BUFFER))

    lat, lon = geocode(place)
    x, y = pyproj.Transformer.from_crs(4326, TARGET_CRS, always_xy=True).transform(lon, lat)
    bbox4326 = gpd.GeoSeries([shapely.box(x-buf, y-buf, x+buf, y+buf)],
                             crs=TARGET_CRS).to_crs(4326).total_bounds

    buildings = get_buildings(bbox4326)
    roads     = overpass_roads(bbox4326)

    tmp, fname = tempfile.gettempdir(), f"{place.replace(' ', '_')}.dxf"
    doc = ezdxf.new(); msp = doc.modelspace(); doc.layers.new("BLDG"); doc.layers.new("ROAD")

    def add_poly(poly): msp.add_lwpolyline(list(poly.exterior.coords), dxfattribs={"layer": "BLDG"})
    for g in buildings.geometry:
        if isinstance(g, Polygon): add_poly(g)
        elif isinstance(g, MultiPolygon): [add_poly(p) for p in g.geoms]

    for ln in roads.geometry:
        msp.add_lwpolyline(list(ln.coords), dxfattribs={"layer": "ROAD"})

    doc.saveas(os.path.join(tmp, fname))
    log.info("[DXF] BLDG=%d ROAD=%d", len(buildings), len(roads))
    return jsonify(status="ok", download_url=f"/files/{fname}")

@app.route("/files/<path:fname>")
def download(fname):
    return send_from_directory(tempfile.gettempdir(), fname, as_attachment=True)

@app.route("/")
def hello():
    return "Up — /dwg?place=Lucknow"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 3000)))
