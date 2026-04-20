"""
Initial Master Locations Script
================================
Fetches all Thai air quality monitoring locations from OpenAQ API,
performs reverse geocoding to map coordinates to provinces/regions,
and saves the result as data/thai_locations.csv.

Usage:
    uv run python init_master_locations.py
"""

import os
import logging

import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Thai province name mapping (Thai -> English) grouped by region
THAI_REGIONS_EN = {
    "North": {"เชียงใหม่": "Chiang Mai", "เชียงราย": "Chiang Rai", "น่าน": "Nan", "พะเยา": "Phayao", "แพร่": "Phrae", "แม่ฮ่องสอน": "Mae Hong Son", "ลำปาง": "Lampang", "ลำพูน": "Lamphun", "อุตรดิตถ์": "Uttaradit"},
    "Northeast": {"กาฬสินธุ์": "Kalasin", "ขอนแก่น": "Khon Kaen", "ชัยภูมิ": "Chaiyaphum", "นครพนม": "Nakhon Phanom", "นครราชสีมา": "Nakhon Ratchasima", "บึงกาฬ": "Bueng Kan", "บุรีรัมย์": "Buri Ram", "มหาสารคาม": "Maha Sarakham", "มุกดาหาร": "Mukdahan", "ยโสธร": "Yasothon", "ร้อยเอ็ด": "Roi Et", "เลย": "Loei", "ศรีสะเกษ": "Si Sa Ket", "สกลนคร": "Sakon Nakhon", "สุรินทร์": "Surin", "หนองคาย": "Nong Khai", "หนองบัวลำภู": "Nong Bua Lamphu", "อำนาจเจริญ": "Amnat Charoen", "อุดรธานี": "Udon Thani", "อุบลราชธานี": "Ubon Ratchathani"},
    "Central": {"กรุงเทพมหานคร": "Bangkok", "กรุงเทพ": "Bangkok", "กำแพงเพชร": "Kamphaeng Phet", "ชัยนาท": "Chai Nat", "นครนายก": "Nakhon Nayok", "นครปฐม": "Nakhon Pathom", "นครสวรรค์": "Nakhon Sawan", "นนทบุรี": "Nonthaburi", "ปทุมธานี": "Pathum Thani", "พระนครศรีอยุธยา": "Phra Nakhon Si Ayutthaya", "พิจิตร": "Phichit", "พิษณุโลก": "Phitsanulok", "เพชรบูรณ์": "Phetchabun", "ลพบุรี": "Lop Buri", "สมุทรปราการ": "Samut Prakan", "สมุทรสงคราม": "Samut Songkhram", "สมุทรสาคร": "Samut Sakhon", "สระบุรี": "Saraburi", "สิงห์บุรี": "Sing Buri", "สุโขทัย": "Sukhothai", "สุพรรณบุรี": "Suphan Buri", "อ่างทอง": "Ang Thong", "อุทัยธานี": "Uthai Thani"},
    "East": {"จันทบุรี": "Chanthaburi", "ฉะเชิงเทรา": "Chachoengsao", "ชลบุรี": "Chon Buri", "ตราด": "Trat", "ปราจีนบุรี": "Prachin Buri", "ระยอง": "Rayong", "สระแก้ว": "Sa Kaeo"},
    "West": {"กาญจนบุรี": "Kanchanaburi", "ตาก": "Tak", "ประจวบคีรีขันธ์": "Prachuap Khiri Khan", "เพชรบุรี": "Phetchaburi", "ราชบุรี": "Ratchaburi"},
    "South": {"กระบี่": "Krabi", "ชุมพร": "Chumphon", "ตรัง": "Trang", "นครศรีธรรมราช": "Nakhon Si Thammarat", "นราธิวาส": "Narathiwat", "ปัตตานี": "Pattani", "พังงา": "Phangnga", "พัทลุง": "Phatthalung", "ภูเก็ต": "Phuket", "ยะลา": "Yala", "ระนอง": "Ranong", "สงขลา": "Songkhla", "สตูล": "Satun", "สุราษฎร์ธานี": "Surat Thani"},
}

# Build flat lookup: Thai province name -> {province_en, region_en}
PROVINCE_MAPPING = {}
for region_en, provinces in THAI_REGIONS_EN.items():
    for prov_th, prov_en in provinces.items():
        PROVINCE_MAPPING[prov_th] = {"province_en": prov_en, "region_en": region_en}


def get_clean_province_and_region(lat, lon, reverse_geocode):
    """Reverse geocode coordinates to Thai province and region names."""
    try:
        location = reverse_geocode(f"{lat}, {lon}", language="th")
        if location:
            full_address = location.address
            for prov_th, info in PROVINCE_MAPPING.items():
                if prov_th in full_address:
                    return info["province_en"], info["region_en"]
    except Exception:
        pass
    return "Unknown", "Unknown"


def main():
    load_dotenv()

    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        raise ValueError("OPENAQ_API_KEY is not set. Please configure it in .env")

    client = OpenAQ(api_key=api_key)
    geolocator = Nominatim(user_agent="thai_pm25_pipeline_v3")
    reverse_geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    logger.info("Fetching Thai monitoring stations from OpenAQ API...")
    response = client.locations.list(countries_id=111, limit=1000)

    clean_locations = []
    skipped = 0

    for loc in response.results:
        if loc.coordinates:
            lat = loc.coordinates.latitude
            lon = loc.coordinates.longitude

            province_en, region_en = get_clean_province_and_region(lat, lon, reverse_geocode)

            if province_en == "Unknown":
                logger.info(f"Skipped foreign/unknown station: {loc.name}")
                skipped += 1
                continue

            clean_locations.append({
                "location_id": loc.id,
                "original_name": loc.name,
                "latitude": lat,
                "longitude": lon,
                "province": province_en,
                "region": region_en,
            })

    df = pd.DataFrame(clean_locations)

    os.makedirs("data", exist_ok=True)
    output_path = "data/thai_locations.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")

    logger.info(f"Saved {len(df)} locations to {output_path} (skipped {skipped} foreign stations)")


if __name__ == "__main__":
    main()
