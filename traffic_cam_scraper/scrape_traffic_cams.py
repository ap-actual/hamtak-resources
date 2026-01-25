import requests
import html
import os
import zipfile
import uuid
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor

# Silencing warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
JSON_URL = "https://chartimap1.sha.maryland.gov/arcgis/rest/services/CHART/Cameras/MapServer/0/query?where=1%3D1&outFields=*&f=json"
OUTPUT_PACKAGE = "MD_Traffic_Cameras_Live.zip"
MAX_WORKERS = 8 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://chart.maryland.gov/",
}

def get_real_name(attrs):
    keys = ['LOCATION_DESCRIPTION', 'STATION_DESCRIPTION', 'LABEL', 'location']
    for key in keys:
        val = attrs.get(key)
        if val and str(val).strip() and str(val).lower() != 'camera':
            return str(val).strip()
    return f"Camera {attrs.get('ID', 'Unknown')}"

def check_camera(cam):
    """Pings camera stream and prepares data for CoT conversion."""
    attrs = cam.get('attributes', {})
    cam_id = attrs.get('CCTV_ID') or attrs.get('ID')
    name = get_real_name(attrs)
    
    if not cam_id: return None

    stream_url = f"https://strmr5.sha.maryland.gov/rtplive/{cam_id}/playlist.m3u8"
    is_working = True 

    try:
        res = requests.get(stream_url, timeout=5, headers=HEADERS, stream=True, verify=False)
        if res.status_code in [404, 503]:
            is_working = False
        res.close()
    except Exception:
        is_working = False

    if is_working:
        print(f"Checked: {name[:45]:<45} | Status: LIVE")
        cam['clean_name'] = name
        cam['stream_url'] = stream_url
        return cam
    return None

def create_cot_sensor(cam_uid, video_uid, name, lat, lon):
    name = html.escape(name)
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid="{cam_uid}" type="b-m-p-s-p-loc" '
            f'time="2026-01-25T16:00:00Z" start="2026-01-25T16:00:00Z" '
            f'stale="2026-02-01T16:00:00Z" how="h-g-i-g-o">'
            f'<point lat="{lat}" lon="{lon}" hae="0" ce="9999999" le="9999999" />'
            f'<detail><contact callsign="{name}" />'
            f'<sensor fov="0" range="0" azimuth="0" displayMagneticReference="0" hideFov="true" />'
            f'<__video uid="{video_uid}" /></detail></event>')

def create_cot_video(video_uid, name, url):
    name = html.escape(name)
    # Parsing URL into components for the ConnectionEntry
    from urllib.parse import urlparse
    u = urlparse(url)
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid="{video_uid}" type="b-i-v" '
            f'time="2026-01-25T16:00:00Z" start="2026-01-25T16:00:00Z" '
            f'stale="2026-01-25T17:00:00Z" how="m-g">'
            f'<point lat="0" lon="0" hae="0" ce="9999999" le="9999999" />'
            f'<detail><contact callsign="{name}" />'
            f'<__video><ConnectionEntry protocol="{u.scheme}" path="{u.path}" address="{u.netloc}" '
            f'port="443" uid="{video_uid}" alias="{name}" roverPort="-1" rtspReliable="0" '
            f'ignoreEmbeddedKLV="False" networkTimeout="12000" bufferTime="-1" />'
            f'</__video></detail></event>')

def create_data_package():
    print("Fetching MD Camera Data...")
    try:
        response = requests.get(JSON_URL, timeout=15, headers=HEADERS, verify=False)
        camera_list = response.json().get('features', [])
    except Exception as e:
        print(f"Fetch Error: {e}"); return

    print(f"Pinging {len(camera_list)} streams with {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = [c for c in list(executor.map(check_camera, camera_list)) if c]

    zip_contents = []
    manifest_entries = []

    for cam in results:
        geom = cam.get('geometry', {})
        lon, lat = geom.get('x'), geom.get('y')
        name = cam['clean_name']
        
        s_uid, v_uid = str(uuid.uuid4()), str(uuid.uuid4())
        s_path, v_path = f"{s_uid}/{s_uid}.cot", f"{v_uid}/{v_uid}.cot"

        zip_contents.append((s_path, create_cot_sensor(s_uid, v_uid, name, lat, lon)))
        zip_contents.append((v_path, create_cot_video(v_uid, name, cam['stream_url'])))
        
        manifest_entries.append({'path': s_path, 'uid': s_uid, 'type': 'Sensor'})
        manifest_entries.append({'path': v_path, 'uid': v_uid, 'type': 'Video'})

    # Manifest generation
    manifest_xml = (f'<?xml version="1.0" encoding="UTF-8"?>'
                    f'<MissionPackageManifest version="2">'
                    f'<Configuration>'
                    f'<Parameter name="uid" value="{uuid.uuid4()}"/>'
                    f'<Parameter name="name" value="MD_Traffic_Live_DataPackage"/>'
                    f'</Configuration><Contents>')
    
    for entry in manifest_entries:
        ctype = ' <Parameter name="contentType" value="Video"/>' if entry['type'] == 'Video' else ''
        manifest_xml += (f'<Content ignore="false" zipEntry="{entry["path"]}">'
                         f'<Parameter name="uid" value="{entry["uid"]}"/>{ctype}</Content>')
    manifest_xml += '</Contents></MissionPackageManifest>'

    # Create Zip Package
    with zipfile.ZipFile(OUTPUT_PACKAGE, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr("MANIFEST/manifest.xml", manifest_xml.encode('utf-8'))
        for path, data in zip_contents:
            zipf.writestr(path, data.encode('utf-8'))

    print(f"\nSUCCESS: {OUTPUT_PACKAGE} created with {len(results)} live cameras.")

if __name__ == "__main__":
    create_data_package()